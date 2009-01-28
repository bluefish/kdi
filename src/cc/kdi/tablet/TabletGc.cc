//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-08
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

#include <kdi/tablet/TabletGc.h>
#include <warp/log.h>
#include <kdi/scan_predicate.h>
#include <kdi/meta/meta_util.h>
#include <warp/fs.h>
#include <warp/dir.h>
#include <warp/config.h>
#include <warp/uri.h>
#include <warp/interval.h>
#include <warp/memfile.h>
#include <ex/exception.h>

#include <algorithm>
#include <boost/algorithm/string.hpp>

using namespace kdi::tablet;
using namespace kdi;
using namespace warp;
using namespace ex;
using namespace std;

// Notes:
//
// This approach is probably sufficient for single-server setups.  For
// large file sets and distributed tables, we might need to do this in
// stages.  One obvious way to scale this is by partitioning the
// candidate set, then building the active set through a filter
// (e.g. only include a file in the active set if it hits the Bloom
// filter of the candidate set).

namespace {
#if 0
}
#endif

/// Given a table directory path and the server's base data directory,
/// extract the name of the table associated with the directory.
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

/// Scan a tablet server's data directory for garbage candidate files.
/// Only files older than beforeTime are considered.  As files are
/// discovered, they are added to the candidate set.  Also, a set of
/// table names is built from the names of each directory containing
/// data files.
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
        {
            //cerr << "Ignoring: " << path << "   " << mtime << endl;
            continue;
        }
        
        if(!addedTable)
        {
            string tableName = extractTableName(scanDir, dataDir);
            tableSet.push_back(tableName);
            //cerr << "Table: " << tableName << endl;
            addedTable = true;
        }
        
        candidateSet.push_back(path);
        //cerr << "Candidate: " << path << "   " << mtime << endl;
    }
}

/// Given a fragment URI from a tablet config and the server data
/// directory, figure out the file path for the fragment file.
std::string extractFragmentFile(std::string const & fragmentUri,
                                std::string const & dataDir)
{
    Uri u(fragmentUri);
    return fs::resolve(
        dataDir,
        string(u.popScheme().begin(), u.path.end())
        );
}

/// Add all the files referenced by fragments in a tablet config to
/// the active set.
void buildActiveSetFromConfig(
    Config const & config,
    std::string const & dataDir,
    std::vector<std::string> & activeSet)
{
    Config const * tables = config.findChild("tables");
    if(!tables)
        return;

    for(size_t i = 0; i < tables->numChildren(); ++i)
    {
        string path = extractFragmentFile(tables->getChild(i).get(), dataDir);
        activeSet.push_back(path);
        //cerr << "Active: " << path << endl;
    }
}

/// Build a scan predicate to retrieve the tablet configs for each
/// named table from the META table.
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
            .setLowerBound(kdi::meta::encodeMetaRow(*ti, ""), BT_INCLUSIVE)
            .setUpperBound(kdi::meta::encodeLastMetaRow(*ti), BT_INCLUSIVE)
            );
    }

    pred.setRowPredicate(rows);
    return pred;
}

/// Scan the META table for the configs of the named tables.  For each
/// config (that matches the serverRestriction, if any), add all
/// referenced files to the active set.
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

/// Look for Table "state" files in the data directory for each named
/// table.  If the state file exists and contains a valid config, add
/// it and all the fragment files it references to the active set.
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

/// Sort a vector of strings and remove all duplicates.
void sortAndUnique(std::vector<std::string> & vec)
{
    std::sort(vec.begin(), vec.end());
    vec.erase(
        std::unique(vec.begin(), vec.end()),
        vec.end());
}

#if 0
namespace {
#endif
}

//----------------------------------------------------------------------------
// TabletGc
//----------------------------------------------------------------------------
void TabletGc::findTabletGarbage(
    std::string const & dataDir,
    kdi::TablePtr const & metaTable,
    warp::Timestamp const & beforeTime,
    std::string const & serverRestriction,
    std::vector<std::string> & garbageFiles)
{
    vector<string> candidateSet;
    vector<string> tableSet;
    vector<string> activeSet;

    // Scan the data directory to get candidate files and the set of
    // table names
    buildCandidateSet(dataDir, dataDir, beforeTime, candidateSet, tableSet);
    
    // Using the table names, build the active set from state files in
    // the data directory
    buildActiveSetFromState(dataDir, tableSet, activeSet);

    // Augment the active set with files referenced in the META table
    // for each table
    buildActiveSetFromMeta(metaTable, dataDir, serverRestriction,
                           tableSet, activeSet);

    // Make the "sets" into sets
    sortAndUnique(candidateSet);
    sortAndUnique(activeSet);

    // The garbage set is the candidate set minus the active set
    std::set_difference(
        candidateSet.begin(), candidateSet.end(),
        activeSet.begin(), activeSet.end(),
        std::back_inserter(garbageFiles));
}

TabletGc::TabletGc(std::string const & dataDir,
                   kdi::TablePtr const & metaTable,
                   warp::Timestamp const & beforeTime,
                   std::string const & serverRestriction) :
    dataDir(dataDir),
    metaTable(metaTable),
    beforeTime(beforeTime),
    serverRestriction(serverRestriction)
{
}

void TabletGc::run() const
{
    log("Tablet GC: computing garbage set");

    vector<string> garbageFiles;
    findTabletGarbage(dataDir, metaTable, beforeTime,
                      serverRestriction, garbageFiles);
    
    log("Tablet GC: found %d garbage file(s)", garbageFiles.size());
    
    // Print and/or delete each file in the garbage set
    for(vector<string>::const_iterator i = garbageFiles.begin();
        i != garbageFiles.end(); ++i)
    {
        log("Tablet GC: deleting %s", *i);
        fs::remove(*i);
    }

    log("Tablet GC: complete, deleted %d file(s)", garbageFiles.size());
}
