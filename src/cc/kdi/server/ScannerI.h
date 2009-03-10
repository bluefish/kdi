//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-02-27
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

#ifndef KDI_SERVER_SCANNERI_H
#define KDI_SERVER_SCANNERI_H

#include <kdi/rpc/TabletServer.h>
#include <kdi/server/Scanner.h>
#include <boost/noncopyable.hpp>

namespace kdi {
namespace server {

    class ScannerI;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// ScannerI
//----------------------------------------------------------------------------
class kdi::server::ScannerI
    : public ::kdi::rpc::Scanner,
      private boost::noncopyable
{
public:
    // RPC upcall implementation

    typedef ::kdi::rpc::AMD_Scanner_scanMorePtr      RpcScanMoreCbPtr;
    typedef ::kdi::rpc::AMD_Scanner_closePtr         RpcCloseCbPtr;
    typedef ::kdi::rpc::ScanMode                     RpcScanMode;
    typedef ::kdi::rpc::ScanParams                   RpcScanParams;
    typedef ::kdi::rpc::ScanResult                   RpcScanResult;
    typedef ::std::pair<Ice::Byte const *,
                        Ice::Byte const *>           RpcPackedCells;

    void scanMore_async(RpcScanMoreCbPtr const & cb,
                        RpcScanParams const & params,
                        Ice::Current const & cur);

    void close_async(RpcCloseCbPtr const & cb,
                     Ice::Current const & cur);


public:
    ScannerI(::kdi::server::ScannerPtr const & scanner,
             ::kdi::server::ScannerLocator * locator) :
        scanner(scanner),
        locator(locator)
    {
    }

private:
    ::kdi::server::ScannerPtr scanner;
    ::kdi::server::ScannerLocator * locator;
};


#endif // KDI_SERVER_SCANNERI_H
