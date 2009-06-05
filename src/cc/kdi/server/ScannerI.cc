//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-07
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

#include <kdi/server/ScannerI.h>
#include <kdi/server/ScannerLocator.h>
#include <warp/log.h>

using namespace kdi;
using namespace kdi::server;
using warp::log;

//----------------------------------------------------------------------------
// ScannerI::ScanCb
//----------------------------------------------------------------------------
class ScannerI::ScanCb
    : public ::kdi::server::Scanner::ScanCb
{
public:
    ScanCb(ScannerI * scannerI, RpcScanMoreCbPtr const & cb,
           bool forceClose) :
        scannerI(scannerI),
        cb(cb),
        forceClose(forceClose)
    {
    }
    
    void done() throw()
    {
        ::kdi::server::ScannerPtr const & scanner = scannerI->scanner;

        strref_t cells = scanner->getPackedCells();

        ::kdi::rpc::ScanResult result;
        result.scanSeq = scannerI->scanSeq;
        result.scanTxn = scanner->getScanTransaction();
        result.scanComplete = !scanner->scanContinues();
        result.scanClosed = (result.scanComplete || forceClose);

        if(result.scanClosed)
            scannerI->doClose();

        resetUseFlag();

        //log("ScannerI %d seq %d: scanMore response",
        //    scannerI->scanId, result.scanSeq);
        cb->ice_response(
            RpcPackedCells(
                reinterpret_cast<Ice::Byte const *>(cells.begin()),
                reinterpret_cast<Ice::Byte const *>(cells.end())),
            result);

        delete this;
    }

    void error(std::exception const & err) throw()
    {
        cb->ice_exception(err);
        resetUseFlag();
        delete this;
    }

private:
    void resetUseFlag()
    {
        boost::mutex::scoped_lock lock(scannerI->mutex);
        scannerI->inUse = false;
        scannerI->scanSeq += 1;
    }

private:
    ScannerI * scannerI;
    RpcScanMoreCbPtr cb;
    bool forceClose;
};


//----------------------------------------------------------------------------
// ScannerI
//----------------------------------------------------------------------------
ScannerI::ScannerI(::kdi::server::ScannerPtr const & scanner,
                   ::kdi::server::ScannerLocator * locator,
                   size_t scanId) :
    scanner(scanner),
    locator(locator),
    scanId(scanId),
    scanSeq(1),
    inUse(false)
{
}

void ScannerI::scanMore_async(RpcScanMoreCbPtr const & cb,
                              RpcScanParams const & params,
                              Ice::Current const & cur)
{
    //log("ScannerI %d seq r=%d e=%d: scanMore",
    //    scanId, params.scanSeq, scanSeq);

    boost::mutex::scoped_lock lock(mutex);
    if(params.scanSeq != scanSeq)
    {
        log("ScannerI %d: seq mismatch, requested %d, expected %d",
            scanId, params.scanSeq, scanSeq);
        cb->ice_exception(::kdi::rpc::ScannerSequenceError());
        return;
    }
    if(inUse)
    {
        log("ScannerI %d: seq %d, already in use",
            scanId, scanSeq);
        cb->ice_exception(::kdi::rpc::ScannerBusyError());
        return;
    }
    inUse = true;
    lock.unlock();

    scanner->scan_async(
        new ScanCb(this, cb, params.close),
        params.maxCells,
        params.maxSize);
}

void ScannerI::close_async(RpcCloseCbPtr const & cb,
                           Ice::Current const & cur)
{
    //log("ScannerI %d: close", scanId);

    boost::mutex::scoped_lock lock(mutex);
    if(inUse)
    {
        cb->ice_exception(::kdi::rpc::ScannerBusyError());
        return;
    }

    doClose();

    //log("ScannerI %d: close response", scanId);
    cb->ice_response();
}

void ScannerI::doClose()
{
    locator->remove(scanId);
}
