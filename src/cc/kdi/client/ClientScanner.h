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

#ifndef KDI_CLIENT_CLIENTSCANNER_H
#define KDI_CLIENT_CLIENTSCANNER_H

#include <kdi/cell.h>
#include <kdi/rpc/TabletServer.h>
#include <kdi/rpc/PackedCellReader.h>
#include <vector>
#include <string>

namespace kdi {
namespace client {

    class ClientScanner;

} // namespace client

    // Forward declarations
    class ScanPredicate;

} // namespace kdi

//----------------------------------------------------------------------------
// ClientScanner
//----------------------------------------------------------------------------
class kdi::client::ClientScanner
    : public CellStream
{
public:
    ClientScanner(kdi::rpc::TabletServerPrx const & server,
                  std::string const & tableName,
                  ScanPredicate const & pred);
    ~ClientScanner();

    bool get(Cell & x);

private:
    void resetReader();

private:
    kdi::rpc::ScannerPrx scanner;
    kdi::rpc::PackedCells packed;
    kdi::rpc::PackedCellReader reader;
    kdi::rpc::ScanResult result;
    int64_t scanSeq;
};


#endif // KDI_CLIENT_CLIENTSCANNER_H
