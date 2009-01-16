//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-11-04
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

#include <kdi/tablet/CachedLogLoader.h>
#include <kdi/tablet/ConfigManager.h>
#include <kdi/local/disk_table_writer.h>
#include <kdi/local/disk_table.h>
#include <kdi/tablet/LogReader.h>
#include <kdi/cell.h>
#include <warp/uri.h>
#include <warp/log.h>
#include <ex/exception.h>
#include <algorithm>
#include <vector>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace ex;
using namespace std;

using kdi::local::DiskTableWriter;
using kdi::local::DiskTable;

//----------------------------------------------------------------------------
// CachedLogLoader::LogInfo
//----------------------------------------------------------------------------
struct CachedLogLoader::LogInfo
{
    std::string diskUri;
    boost::mutex mutex;

    void serializeLog(std::string const & logUri,
                      boost::mutex & writerMutex,
                      FragmentWriter * writer);
};


//----------------------------------------------------------------------------
// CachedLogLoader::LogInfo
//----------------------------------------------------------------------------
void CachedLogLoader::LogInfo::serializeLog(std::string const & logUri,
                                            boost::mutex & writerMutex,
                                            FragmentWriter * writer)
{
    // Get the tablet name from the URI.
    string tableName = uriDecode(
        uriGetParameter(logUri, "table"),
        true);

    log("CachedLogLoader: loading table %s from log %s", tableName, logUri);

    // Read the log cells into a vector
    vector<Cell> cells;
    try {
        LogReader reader(
            uriPushScheme(uriPopScheme(logUri), "cache"),
            tableName);
        Cell x;
        while(reader.get(x))
            cells.push_back(x);
    }
    catch(std::exception const & ex) {
        log("ERROR: failed to load log %s: %s", logUri, ex.what());
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
    {
        boost::mutex::scoped_lock writerLock(writerMutex);

        writer->start(tableName);
        for(; first != cells.end(); ++first)
            writer->put(*first);
        diskUri = writer->finish();
    }

    log("CachedLogLoader: wrote new table %s", diskUri);
}


//----------------------------------------------------------------------------
// CachedLogLoader
//----------------------------------------------------------------------------
CachedLogLoader::CachedLogLoader(FragmentLoader * loader,
                                 FragmentWriter * writer) :
    loader(loader),
    writer(writer)
{
    EX_CHECK_NULL(loader);
    EX_CHECK_NULL(writer);
}

FragmentPtr CachedLogLoader::load(std::string const & uri) const
{
    if(uriTopScheme(uri) != "sharedlog")
        raise<ValueError>("invalid URI for CachedLogLoader: %s", uri);

    return loader->load(getDiskUri(uri));
}

std::string const & CachedLogLoader::getDiskUri(std::string const & logUri) const
{
    boost::mutex::scoped_lock mapLock(mutex);
    boost::shared_ptr<LogInfo> & info = logMap[logUri];
    if(!info)
        info.reset(new LogInfo);
    mapLock.unlock();

    boost::mutex::scoped_lock logLock(info->mutex);
    if(info->diskUri.empty())
        info->serializeLog(logUri, writerMutex, writer);

    return info->diskUri;
}
