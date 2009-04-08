//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-02-25
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

#ifndef KDI_SERVER_TABLETSERVERI_H
#define KDI_SERVER_TABLETSERVERI_H

#include <kdi/rpc/TabletServer.h>
#include <kdi/server/ScannerLocator.h>
#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>

namespace kdi {
namespace server {

    class TabletServerI;

    // Forward declarations
    class TabletServer;
    class BlockCache;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// TabletServerI
//----------------------------------------------------------------------------
class kdi::server::TabletServerI
    : public ::kdi::rpc::TabletServer,
      private boost::noncopyable
{
public:
    TabletServerI(::kdi::server::TabletServer * server,
                  ::kdi::server::ScannerLocator * locator,
                  ::kdi::server::BlockCache * cache);

public:
    // RPC upcall implementation

    typedef ::kdi::rpc::AMD_TabletServer_applyPtr    RpcApplyCbPtr;
    typedef ::kdi::rpc::AMD_TabletServer_syncPtr     RpcSyncCbPtr;
    typedef ::kdi::rpc::AMD_TabletServer_scanPtr     RpcScanCbPtr;
    typedef ::kdi::rpc::ScanMode                     RpcScanMode;
    typedef ::kdi::rpc::ScanParams                   RpcScanParams;
    typedef ::std::pair<Ice::Byte const *,
                        Ice::Byte const *>           RpcPackedCells;
    typedef ::std::string                            RpcString;

    void apply_async(RpcApplyCbPtr const & cb,
                     RpcString const & table,
                     RpcPackedCells const & cells,
                     int64_t commitMaxTxn,
                     bool waitForSync,
                     Ice::Current const & cur);
    
    void sync_async(RpcSyncCbPtr const & cb,
                    int64_t waitForTxn,
                    Ice::Current const & cur);

    void scan_async(RpcScanCbPtr const & cb,
                    RpcString const & table,
                    RpcString const & predicate,
                    RpcScanMode mode,
                    RpcScanParams const & params,
                    Ice::Current const & cur);

private:
    class ApplyCb;
    class SyncCb;
    class ScanCb;

    ::kdi::server::TabletServer *   const server;
    ::kdi::server::ScannerLocator * const locator;
    ::kdi::server::BlockCache *     const cache;

    boost::mutex scannerIdMutex;
    size_t nextScannerId;
};

#endif // KDI_SERVER_TABLETSERVERI_H
