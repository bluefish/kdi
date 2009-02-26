//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/server/TabletServer.h $
//
// Created 2009/02/25
//
// Copyright 2009 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_SERVER_TABLETSERVER_H
#define KDI_SERVER_TABLETSERVER_H

#include <boost/noncopyable.hpp>
#include <kdi/rpc/TableServer.h>

namespace kdi {
namespace server {

    class TabletServerI;
    class TabletServer;

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
    // RPC upcall implementation

    typedef ::kdi::rpc::AMD_TabletServer_applyPtr    RpcApplyCbPtr;
    typedef ::kdi::rpc::AMD_TabletServer_syncPtr     RpcSyncCbPtr;
    typedef ::kdi::rpc::AMD_TabletServer_scanPtr     RpcScanCbPtr;
    typedef ::kdi::rpc::ScanMode                     RpcScanMode;
    typedef ::kdi::rpc::ScanParams                   RpcScanParams;
    typedef ::std::pair<char const *,char const *>   RpcPackedCells;
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

    TabletServer * server;
    ScannerLocator * locator;
};

#endif // KDI_SERVER_TABLETSERVER_H
