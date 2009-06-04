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

#include <kdi/server/TabletServerI.h>
#include <kdi/server/ScannerI.h>
#include <kdi/server/TabletServer.h>
#include <kdi/server/Scanner.h>
#include <kdi/server/ScannerLocator.h>
#include <kdi/server/errors.h>
#include <warp/log.h>
#include <Ice/Ice.h>

using namespace kdi;
using namespace kdi::server;
using warp::log;

//----------------------------------------------------------------------------
// TabletServerI:ApplyCb
//----------------------------------------------------------------------------
class TabletServerI::ApplyCb
    : public ::kdi::server::TabletServer::ApplyCb
{
    RpcApplyCbPtr cb;

public:
    ApplyCb(RpcApplyCbPtr const & cb) :
        cb(cb) {}

    void done(int64_t commitTxn)
    {
        cb->ice_response(commitTxn);
        delete this;
    }

    void error(std::exception const & err)
    {
        // Translate app exceptions to RPC exceptions
        try {
            throw err;
        }

        catch(::kdi::server::NotLoadedError const &) {
            cb->ice_exception(::kdi::rpc::TabletNotLoadedError());
        }

        catch(::kdi::server::BadOrderError const &) {
            cb->ice_exception(::kdi::rpc::CellDisorderError());
        }
        catch(::kdi::server::BadMagicError const &) {
            cb->ice_exception(::kdi::rpc::InvalidPackedFormatError());
        }
        catch(::kdi::server::BadChecksumError const &) {
            cb->ice_exception(::kdi::rpc::InvalidPackedFormatError());
        }
        catch(::kdi::server::UnknownColumnFamilyError const &) {
            cb->ice_exception(::kdi::rpc::UnknownColumnFamilyError());
        }
        catch(::kdi::server::CellDataError const &) {
            cb->ice_exception(::kdi::rpc::InvalidCellsError());
        }

        catch(::kdi::server::MutationConflictError const &) {
            cb->ice_exception(::kdi::rpc::MutationConflictError());
        }
        catch(::kdi::server::TransactionError const &) {
            cb->ice_exception(::kdi::rpc::TransactionError());
        }

        catch(...) {
            cb->ice_exception(err);
        }
        delete this;
    }
};

//----------------------------------------------------------------------------
// TabletServerI::SyncCb
//----------------------------------------------------------------------------
class TabletServerI::SyncCb
    : public ::kdi::server::TabletServer::SyncCb
{
    RpcSyncCbPtr cb;

public:
    SyncCb(RpcSyncCbPtr const & cb) :
        cb(cb) {}

    void done(int64_t scanTxn)
    {
        cb->ice_response(scanTxn);
        delete this;
    }

    void error(std::exception const & err)
    {
        // Should translate app exceptions to RPC exceptions
        cb->ice_exception(err);
        delete this;
    }
};

//----------------------------------------------------------------------------
// TabletServerI::ScanCb
//----------------------------------------------------------------------------
class TabletServerI::ScanCb
    : public Scanner::ScanCb
{
    RpcScanCbPtr cb;
    ScannerPtr scanner;
    size_t scanId;
    ScannerLocator * locator;
    Ice::ObjectAdapterPtr adapter;

public:
    ScanCb(RpcScanCbPtr const & cb,
           ScannerPtr const & scanner,
           size_t scanId,
           ScannerLocator * locator,
           Ice::ObjectAdapterPtr const & adapter) :
        cb(cb),
        scanner(scanner),
        scanId(scanId),
        locator(locator),
        adapter(adapter)
    {
    }

    void done()
    {
        strref_t cells = scanner->getPackedCells();
        ::kdi::rpc::ScanResult result;
        result.scanSeq = 0;
        result.scanTxn = scanner->getScanTransaction();
        result.scanComplete = !scanner->scanContinues();
        result.scanClosed = true;

        // If there's more to scan, set up a continuation object
        ::kdi::rpc::ScannerPrx scanContinuation;
        if(locator && !result.scanComplete)
        {
            // Make identity
            Ice::Identity id;
            id.category = "scan";
            id.name = locator->getNameFromId(scanId);

            // Make ICE object for scanner
            Ice::ObjectPtr obj = new ScannerI(scanner, locator, scanId);

            // Add it to the scanner locator
            locator->add(scanId, obj);

            // Set the continuation proxy
            scanContinuation = ::kdi::rpc::ScannerPrx::uncheckedCast(
                adapter->createProxy(id));

            // Note that the scanner is open
            result.scanClosed = false;
        }

        cb->ice_response(
            RpcPackedCells(
                reinterpret_cast<Ice::Byte const *>(cells.begin()),
                reinterpret_cast<Ice::Byte const *>(cells.end())),
            result,
            scanContinuation);
        delete this;
    }

    void error(std::exception const & err)
    {
        // Should translate app exceptions to RPC exceptions
        cb->ice_exception(err);
        delete this;
    }
};


//----------------------------------------------------------------------------
// TabletServerI
//----------------------------------------------------------------------------
TabletServerI::TabletServerI(::kdi::server::TabletServer * server,
                             ::kdi::server::ScannerLocator * locator,
                             ::kdi::server::BlockCache * cache) :
    server(server),
    locator(locator),
    cache(cache),
    nextScannerId(0)
{
}

void TabletServerI::apply_async(
    RpcApplyCbPtr const & cb,
    RpcString const & table,
    RpcPackedCells const & cells,
    int64_t commitMaxTxn,
    bool waitForSync,
    Ice::Current const & cur)
{
    //log("TabletServerI: apply %s, %d bytes, maxTxn=%d, wait=%s",
    //    table, (cells.second - cells.first), commitMaxTxn, waitForSync);

    server->apply_async(
        new ApplyCb(cb),
        table,
        warp::binary_data(cells.first, cells.second - cells.first),
        commitMaxTxn,
        waitForSync);
}

void TabletServerI::sync_async(
    RpcSyncCbPtr const & cb,
    int64_t waitForTxn,
    Ice::Current const & cur)
{
    log("TabletServerI: sync waitTxn=%d", waitForTxn);

    server->sync_async(new SyncCb(cb), waitForTxn);
}

void TabletServerI::scan_async(
    RpcScanCbPtr const & cb,
    RpcString const & table,
    RpcString const & predicate,
    RpcScanMode mode,
    RpcScanParams const & params,
    Ice::Current const & cur)
{
    log("TabletServerI: scan %s, pred=(%s)", table, predicate);

    // Translate scan mode
    Scanner::ScanMode m;
    switch(mode)
    {
        case rpc::kScanAnyTxn:
            m = Scanner::SCAN_ANY_TXN;
            break;
        case rpc::kScanIsolatedRowTxn:
            m = Scanner::SCAN_ISOLATED_ROW_TXN;
            break;
        case rpc::kScanLatestRowTxn:
            m = Scanner::SCAN_LATEST_ROW_TXN;
            break;
        default:
            cb->ice_exception(::kdi::rpc::ScanModeError());
            return;
    }

    // Validate scan sequence
    if(params.scanSeq != 0)
    {
        cb->ice_exception(::kdi::rpc::ScannerSequenceError());
        return;
    }

    // Parse the scan predicate
    ScanPredicate pred;
    try {
        pred = ScanPredicate(predicate);
    }
    catch(...) {
        cb->ice_exception(::kdi::rpc::InvalidPredicateError());
        return;
    }

    // Get the table
    TabletServerLock serverLock(server);
    Table * t = server->findTable(table);
    if(!t)
    {
        cb->ice_exception(::kdi::rpc::TabletNotLoadedError());
        return;
    }

    // Get a scanner ID
    size_t scanId;
    {
        boost::mutex::scoped_lock lock(scannerIdMutex);
        scanId = nextScannerId++;
    }

    // Make a scanner
    ScannerPtr scanner(new Scanner(t, cache, pred, m));
    serverLock.unlock();

    // Start the scan
    scanner->scan_async(
        new ScanCb(
            cb,
            scanner,
            scanId,
            params.close ? 0 : locator,
            cur.adapter
            ),
        params.maxCells,
        params.maxSize);
}
