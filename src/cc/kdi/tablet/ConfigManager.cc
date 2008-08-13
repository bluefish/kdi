//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/ConfigManager.cc $
//
// Created 2008/08/07
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/tablet/ConfigManager.h>
#include <kdi/tablet/DiskFragment.h>
#include <kdi/local/disk_table_writer.h>
#include <kdi/local/disk_table.h>
#include <kdi/tablet/LogReader.h>
#include <kdi/cell.h>
#include <warp/uri.h>
#include <warp/log.h>
#include <ex/exception.h>
#include <vector>
#include <algorithm>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace std;
using namespace ex;

using kdi::local::DiskTableWriter;
using kdi::local::DiskTable;

//----------------------------------------------------------------------------
// ConfigManager
//----------------------------------------------------------------------------
FragmentPtr ConfigManager::openFragment(std::string const & uri)
{
    log("ConfigManager: openFragment %s", uri);

    string scheme = uriTopScheme(uri);
    if(scheme == "disk")
    {
        // Disk table: open and return
        FragmentPtr p(new DiskFragment(uri));
        return p;
    }
    else if(scheme == "sharedlog")
    {
        // Log table: serialize to a disk table and return that.

        // Get the tablet name from the URI.
        string tableName = uriDecode(
            uriGetParameter(uri, "table"),
            true);

        log("ConfigManager: loading table %s from log %s", tableName, uri);

        // XXX: we should cache logs we've already translated

        // Read the log cells into a vector
        vector<Cell> cells;
        {
            LogReader reader(
                uriPushScheme(uriPopScheme(uri), "cache"),
                tableName);
            Cell x;
            while(reader.get(x))
                cells.push_back(x);
        }

        // Sort the cells (use a stable sort since ordering between
        // duplicates is important)
        std::stable_sort(cells.begin(), cells.end());

        // Unique the cells.  We want the last cell of each duplicate
        // sequence instead of the first, so we'll unique using
        // reverse iterators.  The end returned by unique will be the
        // first cell to keep in the forward iterator world.
        vector<Cell>::iterator first = std::unique(
            cells.rbegin(), cells.rend()
            ).base();

        // Write the remaining cells to a disk table
        string diskFn = getDataFile(tableName);
        {
            log("ConfigManager: writing new table %s", diskFn);

            DiskTableWriter writer(diskFn, 64<<10);
            for(; first != cells.end(); ++first)
                writer.put(*first);
            writer.close();
        }

        // Return new table
        FragmentPtr p(
            new DiskFragment(
                uriPushScheme(diskFn, "disk")
                )
            );
        return p;
    }

    // Unknown scheme
    raise<RuntimeError>("unknown fragment scheme %s: %s", scheme, uri);
}
