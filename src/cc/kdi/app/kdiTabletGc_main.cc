//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-10-31
// 
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the Free Software
// Foundation; either version 2 of the License, or any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
// details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#include <kdi/table.h>
#include <kdi/scan_predicate.h>
#include <kdi/meta/meta_util.h>
#include <warp/timestamp.h>
#include <warp/fs.h>
#include <warp/dir.h>
#include <warp/config.h>
#include <warp/uri.h>
#include <warp/interval.h>
#include <warp/memfile.h>
#include <ex/exception.h>

#include <warp/options.h>
#include <string>
#include <vector>
#include <algorithm>
#include <iostream>
#include <boost/algorithm/string.hpp>

using namespace warp;
using namespace ex;
using namespace kdi;
//using namespace kdi::tablet;
using namespace std;

// Notes:
//
// This approach is probably sufficient for single-server setups.  For
// large file sets and distributed tables, we might need to do this in
// stages.  One obvious way to scale this is by partitioning the
// candidate set, then building the active set through a filter
// (e.g. only include a file in the active set if it hits the Bloom
// filter of the candidate set).

std::string extractTableName(std::string const & tableDir,
                             std::string const & dataDir)
{
    Uri tableUri(tableDir);
    Uri dataUri(dataDir);

    using boost::algorithm::starts_with;

    if(!starts_with(tableUri.path, dataUri.path))
        raise<ValueError>("can't get table name: tableDir=%s dataDir=%s",
                          tableDir, dataDir);

    char const * begin = tableUri.path.begin() + dataUri.path.size();
    char const * end = tableUri.path.end();
    while(begin != end && *begin == '/')
        ++begin;
    while(begin != end && end[-1] == '/')
        --end;

    if(begin == end)
        raise<ValueError>("can't get table name: tableDir=%s dataDir=%s",
                          tableDir, dataDir);

    return string(begin, end);
}

void buildCandidateSet(
    std::string const & scanDir,
    std::string const & dataDir,
    warp::Timestamp const & beforeTime,
    std::vector<std::string> & candidateSet,
    std::vector<std::string> & tableSet)
{
    bool addedTable = false;
    DirPtr dir = Directory::open(scanDir);
    for(string path; dir->readPath(path);)
    {
        if(fs::isDirectory(path))
        {
            buildCandidateSet(path, dataDir, beforeTime, candidateSet, tableSet);
            continue;
        }

        Timestamp mtime = Timestamp::fromSeconds(fs::modificationTime(path));
        if(mtime >= beforeTime)
            continue;
        
        if(!addedTable)
        {
            string tableName = extractTableName(scanDir, dataDir);
            tableSet.push_back(tableName);
            //cerr << "Table: " << tableName << endl;
            addedTable = true;
        }
        
        candidateSet.push_back(path);
        //cerr << "Candidate: " << path << endl;
    }
}

std::string extractFragmentFile(std::string const & fragmentUri, std::string const & dataDir)
{
    Uri u(fragmentUri);
    return fs::resolve(
        dataDir,
        string(u.popScheme().begin(), u.path.end())
        );
}

void buildActiveSetFromConfig(
    Config const & config,
    std::string const & dataDir,
    std::vector<std::string> & activeSet)
{
    Config const & tables = config.getChild("tables");
    for(size_t i = 0; i < tables.numChildren(); ++i)
    {
        string path = extractFragmentFile(tables.getChild(i).get(), dataDir);
        activeSet.push_back(path);
        //cerr << "Active: " << path << endl;
    }
}

kdi::ScanPredicate makeMetaScanPredicate(
    std::vector<std::string> const & tableSet)
{
    ScanPredicate pred("column = 'config' and history = 1");

    IntervalSet<string> rows;
    for(vector<string>::const_iterator ti = tableSet.begin();
        ti != tableSet.end(); ++ti)
    {
        rows.add(
            Interval<string>()
            .setLowerBound(kdi::meta::encodeMetaRow(*ti, ""))
            .setUpperBound(kdi::meta::encodeLastMetaRow(*ti))
            );
    }

    pred.setRowPredicate(rows);
    return pred;
}

void buildActiveSetFromMeta(
    kdi::TablePtr const & metaTable,
    std::string const & dataDir,
    std::string const & serverRestriction,
    std::vector<std::string> const & tableSet,
    std::vector<std::string> & activeSet)
{
    CellStreamPtr metaScan = metaTable->scan(makeMetaScanPredicate(tableSet));
    Cell x;
    while(metaScan->get(x))
    {
        StringRange val = x.getValue();
        FilePtr fp(new MemFile(val.begin(), val.size()));
        Config cfg(fp);

        if(serverRestriction.empty() ||
           cfg.get("server") == serverRestriction)
        {
            buildActiveSetFromConfig(cfg, dataDir, activeSet);
        }
    }
}

void buildActiveSetFromState(
    std::string const & dataDir,
    std::vector<std::string> const & tableSet,
    std::vector<std::string> & activeSet)
{
    for(vector<string>::const_iterator ti = tableSet.begin();
        ti != tableSet.end(); ++ti)
    {
        string statePath = fs::resolve(dataDir, *ti + "/state");
        Config cfg;
        try {
            cfg.loadProperties(statePath);
        }
        catch(...) {
            continue;
        }

        if(!cfg.findChild("tables"))
            continue;

        activeSet.push_back(statePath);
        //cerr << "Active: " << statePath << endl;

        buildActiveSetFromConfig(cfg, dataDir, activeSet);
    }
}

void sortAndUnique(std::vector<std::string> & vec)
{
    std::sort(vec.begin(), vec.end());
    vec.erase(
        std::unique(vec.begin(), vec.end()),
        vec.end());
}

void findTabletGarbage(
    std::string const & dataDir,
    kdi::TablePtr const & metaTable,
    warp::Timestamp const & beforeTime,
    std::string const & serverRestriction,
    std::vector<std::string> & garbageFiles)
{
    vector<string> candidateSet;
    vector<string> tableSet;
    vector<string> activeSet;

    buildCandidateSet(dataDir, dataDir, beforeTime, candidateSet, tableSet);
    
    buildActiveSetFromState(dataDir, tableSet, activeSet);
    buildActiveSetFromMeta(metaTable, dataDir, serverRestriction, tableSet, activeSet);

    sortAndUnique(candidateSet);
    sortAndUnique(activeSet);

    std::set_difference(
        candidateSet.begin(), candidateSet.end(),
        activeSet.begin(), activeSet.end(),
        std::back_inserter(garbageFiles));

    // Scan the data directory.  For each directory containing files:
    {
        // Decide the table name from the directory name and add it to
        // the table set

        // Add all files older than --before to the candidate set

        // If the directory contains a state file, try to read it as a
        // config file.  If that works, add the state file and all
        // files it references to the active set
    }

    // Scan the META table for the table set.  For each config:
    {
        // If we have a server restriction and the config is for
        // another server, skip it.

        // Add all files referenced by the config to the active set
    }

    // Find the delete set: candidate set minus the active set
    
    // Delete each file in the delete set (or not, if --dryrun)
}
    
int main(int ac, char ** av)
{
    OptionParser op("%prog [options] --meta=<meta-table> --data=<data-directory>");
    {
        using namespace boost::program_options;
        op.addOption("meta,m", value<string>(), "location of META table");
        op.addOption("data,d", value<string>(), "location of data directory");
        op.addOption("server,s", value<string>(), "restrict to server");
        op.addOption("before,b", value<string>(), "consider only files before "
                     "(argument can be a timestamp or another file)");
        op.addOption("dryrun,n", "don't delete anything, just print");
        op.addOption("verbose,v", "be verbose");
    }

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    bool dryrun = hasopt(opt, "dryrun");
    bool verbose = hasopt(opt, "verbose");

    string metaTable;
    if(!opt.get("meta", metaTable))
        op.error("need --meta");
    else if(verbose)
        cerr << "Using META table: " << metaTable << endl;
    
    string dataDir;
    if(!opt.get("data", dataDir) || !fs::isDirectory(dataDir))
        op.error("need --data directory");
    else if(verbose)
        cerr << "Using data directory: " << dataDir << endl;
    
    string serverRestriction;
    if(opt.get("server", serverRestriction) && verbose)
        cerr << "Using server restriction: " << serverRestriction << endl;

    string beforeStr;
    Timestamp beforeTime;
    if(opt.get("before", beforeStr))
    {
        if(fs::isFile(beforeStr))
        {
            beforeTime = Timestamp::fromSeconds(
                fs::modificationTime(beforeStr));
        }
        else
        {
            beforeTime = Timestamp::fromString(beforeStr);
        }
    }
    else
    {
        beforeTime = Timestamp::now();
    }
    if(verbose)
        cerr << "Filtering files after: " << beforeTime << endl;


    if(verbose)
        cerr << "Searching for garbage files..." << endl;

    vector<string> garbageFiles;
    findTabletGarbage(dataDir, Table::open(metaTable), beforeTime,
                      serverRestriction, garbageFiles);

    if(verbose)
        cerr << "Found " << garbageFiles.size() << " file(s):" << endl;

    for(vector<string>::const_iterator i = garbageFiles.begin();
        i != garbageFiles.end(); ++i)
    {
        if(dryrun)
        {
            cout << *i << endl;
            continue;
        }

        if(verbose)
            cerr << "Deleting: " << *i << endl;

        fs::remove(*i);
    }
}
