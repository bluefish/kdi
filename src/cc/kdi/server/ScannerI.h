//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/server/ScannerI.h $
//
// Created 2009/02/27
//
// Copyright 2009 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
