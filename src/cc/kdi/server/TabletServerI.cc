//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/server/TabletServerI.cc $
//
// Created 2009/02/25
//
// Copyright 2009 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/server/TabletServerI.h>
#include <kdi/server/TabletServer.h>

using namespace kdi;
using namespace kdi::server;

//----------------------------------------------------------------------------
// TabletServerI::ApplyCb
//----------------------------------------------------------------------------
class TabletServerI::ApplyCb
    : public TabletServer::ApplyCb
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
        // Should translate app exceptions to RPC exceptions
        cb->ice_exception(err);
        delete this;
    }
};

//----------------------------------------------------------------------------
// TabletServerI::SyncCb
//----------------------------------------------------------------------------
class TabletServerI::SyncCb
    : public TabletServer::SyncCb
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

public:
    ScanCb(RpcScanCbPtr const & cb,
           ScannerPtr const & scanner,
           size_t scanId,
           ScannerLocator * locator) :
        cb(cb),
        scanner(scanner),
        scanId(scanId),
        locator(locator)
    {
    }

    void done()
    {
        strref_t cells = scanner->getPackedCells();
        ::kdi::rpc::ScanResult result;
        result.scanTxn = scanner->getScanTransaction();
        result.scanComplete = !scanner->scanContinues();
        result.scanClosed = true;

        // If there's more to scan, set up a continuation object
        ScannerPrx scanContinuation;
        if(locator && !result.scanComplete)
        {
            // Make identity
            Ice::Identity id;
            id.category = "scan";
            id.name = scanLocator->getNameFromId(scanId);

            // Make ICE object for scanner
            ScannerIPtr obj = new ScannerI(scanner, locator);

            // Add it to the scanner locator
            scanLocator->add(scanId, obj);

            // Set the continuation proxy
            scanContinuation = ScannerPrx::uncheckedCast(
                cur.adapter->createProxy(id));

            // Note that the scanner is open
            results.scanClosed = false;
        }

        cb->ice_response(
            RpcPackedCells(cells.begin(), cells.end()),
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
void TabletServerI::apply_async(
    RpcApplyCbPtr const & cb,
    RpcString const & table,
    RpcPackedCells const & cells,
    int64_t commitMaxTxn,
    bool waitForSync,
    Ice::Current const & cur)
{
    server->apply_async(
        new ApplyCb(cb),
        table,
        StringRange(cells.first, cells.second),
        commitMaxTxn,
        waitForSync);
}

void TabletServerI::sync_async(
    RpcSyncCbPtr const & cb,
    int64_t waitForTxn,
    Ice::Current const & cur)
{
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
            cb->ice_exception(ScanModeError());
            return;
    }

    // Parse the scan predicate
    ScanPredicate pred;
    try {
        pred = ScanPredicate(predicate);
    }
    catch(...) {
        cb->ice_exception(InvalidPredicateError());
        return;
    }

    // Get the table
    Table * t = server->tryGetTable(table);
    if(!t)
    {
        cb->ice_exception(TabletNotLoadedError());
        return;
    }

    // Get a scanner ID
    size_t scanId = assignScannerId();

    // Make a scanner
    ScannerPtr scanner(new Scanner(t, pred, m));

    // Start the scan
    scanner->scan_async(
        new ScanCb(
            cb,
            scanner,
            scanId,
            params.close ? 0 : locator
            ),
        maxCells,
        maxSize);
}
