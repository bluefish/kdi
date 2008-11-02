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

#include <warp/options.h>
#include <string>

using namespace warp;
//using namespace kdi;
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

    using boost::algorithms::starts_with;

    if(!starts_with(tableUri.path, dataUri.path))
        raise<ValueError>("can't get table name: tableDir=%s dataDir=%s",
                          tableDir, dataDir);

    char const * begin = dataUri.path.begin() + tableUri.path.size();
    char const * end = dataUri.path.end();
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
    Directory dir(scanDir);
    for(std::string path; dir.getPath(path);)
    {
        if(fs::isDirectory(path))
        {
            buildCandidateSet(path, dataDir, beforeTime, candidateSet, tableSet);
            continue;
        }

        Timestamp mtime = Time::fromSeconds(fs::modificationTime(path));
        if(mtime >= beforeTime)
            continue;
        
        if(!addedTable)
        {
            string tableName = extractTableName(path, dataDir);
            tableSet.push_back(tableName);
            addedTable = true;
        }
        
        candidateSet.push_back(path);
    }
}

std::string extractFragmentFile(std::string const & fragmentUri)
{
    Uri u(fragmentUri);
    return string(u.topScheme().begin(), u.path.end());
}

void buildActiveSetFromConfig(
    Config const & config,
    std::vector<std::string> & activeSet)
{
    Config const & tables = config.getChild("tables");
    for(size_t i = 0; i < cfg.getNumChildren(); ++i)
    {
        string path = extractFragmentFile(cfg.getChild(i).get());
        activeSet.push_back(path);
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
            .setLowerBound(encodeMetaRow(*ti, ""))
            .setUpperBound(encodeLastMetaRow(*ti))
            );
    }

    pred.setRowPredicate(rows);
    return pred;
}

void buildActiveSetFromMeta(
    kdi::TablePtr const & metaTable,
    std::string const & serverRestriction,
    std::vector<std::string> const & tableSet,
    std::vector<std::string> & activeSet)
{
    CellStreamPtr metaScan = metaTable.scan(makeMetaScanPredicate(tableSet));
    Cell x;
    while(metaScan->get(x))
    {
        StringRange val = x.getValue();
        FilePtr fp(new MemFile(val.begin(), val.size()));
        Config cfg(fp);

        if(serverRestriction.empty() ||
           cfg.get("server") == serverRestriction)
        {
            buildActiveSetFromConfig(cfg, activeSet);
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
        buildActiveSetFromConfig(cfg, activeSet);
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
    buildActiveSetFromMeta(metaTable, serverRestriction, tableSet, activeSet);

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
