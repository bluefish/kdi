//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-06
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

#include <kdi/client/ClientScanner.h>
#include <kdi/scan_predicate.h>
#include <warp/log.h>
#include <sstream>
#include <exception>

using namespace kdi;
using namespace kdi::client;
using warp::log;

//----------------------------------------------------------------------------
// ClientScanner
//----------------------------------------------------------------------------
ClientScanner::ClientScanner(
    kdi::rpc::TabletServerPrx const & server,
    std::string const & tableName,
    ScanPredicate const & pred)
{
    std::ostringstream oss;
    oss << pred;

    kdi::rpc::ScanParams params;
    params.maxCells = 128<<10;
    params.maxSize = 128<<10;
    params.close = false;

    //log("Scanning: %s", tableName);
    server->scan(tableName, oss.str(),
                 kdi::rpc::kScanAnyTxn, params,
                 packed, result, scanner);
    resetReader();
}

ClientScanner::~ClientScanner()
{
    if(!result.scanClosed)
    {
        try {
            scanner->close();
        }
        catch(...) {}
    }
}

bool ClientScanner::get(Cell & x)
{
    for(;;)
    {
        if(reader.next())
        {
            x = makeCell(reader.getRow(), reader.getColumn(),
                         reader.getTimestamp(), reader.getValue());
            return true;
        }

        if(result.scanClosed)
            return false;

        kdi::rpc::ScanParams params;
        params.maxCells = 128<<10;
        params.maxSize = 128<<10;
        params.close = false;

        //log("Scanning more");
        scanner->scanMore(params, packed, result);
        resetReader();
    }
}

void ClientScanner::resetReader()
{
    //log("Got %d bytes, txn=%d, complete=%s, closed=%s",
    //    packed.size(), result.scanTxn, result.scanComplete,
    //    result.scanClosed);

    reader.reset(warp::binary_data(&packed[0], packed.size()));
    if(!reader.verifyMagic())
        throw std::runtime_error("bad packed cell magic");
    if(!reader.verifyChecksum())
        throw std::runtime_error("bad packed cell checksum");
    if(!reader.verifyOffsets())
        throw std::runtime_error("bad packed cell offsets");
}
