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

#include <kdi/server/TabletServer.h>
#include <kdi/server/CellBuffer.h>
#include <kdi/server/Table.h>
#include <kdi/server/Fragment.h>

using namespace kdi;
using namespace kdi::server;

namespace {
    typedef boost::mutex::scoped_lock lock_t;
}

void TabletServer::apply_async(
    ApplyCb * cb,
    strref_t tableName,
    strref_t packedCells,
    int64_t commitMaxTxn,
    bool waitForSync)
{
    try {
        // Decode and validate cells
        FragmentCPtr fragment = decodeCells(packedCells);

        // Try to apply the commit
        lock_t lock(serverMutex);

        // Find the table
        Table * table = getTable(tableName);

        // Note: if the tablets are assigned to this server but still
        // in the process of being loaded, may want to defer until
        // they are ready.
        
        // Make sure all cells map to loaded tablets and that the
        // transaction will apply.  The transaction can proceed only
        // if the last transaction in each row is less than or equal
        // to commitMaxTxn.
        table->verifyCommitApplies(
            fragment->makeRowIterator(), commitMaxTxn);

        // How to throttle?

        // We don't want to queue up too many heavy-weight objects
        // while waiting to commit to the log.  It would be bad to
        // make a full copy of the input data and queue it.  If we got
        // flooded with mutations we'd either: 1) use way too much
        // memory if we had a unbounded queue, or 2) block server
        // threads and starve readers if we have a bounded queue.
        
        // Ideally, we'd leave mutations "on the wire" until we were
        // ready for them, while allowing read requests to flow
        // independently.

        // If we use the Slice array mapping for the packed cell block
        // (and the backing buffer stays valid for the life of the AMD
        // invocation), then we can queue lightweight objects.  Ice
        // might be smart enough to do flow control for us in this
        // case.  I'm guessing it has a buffer per connection.  If we
        // tie up the buffer space, then the writing connection should
        // stall while allowing other connections to proceed.  Of
        // course interleaved reads on the same connection will
        // suffer, but that's not as big of a concern.

        // Throttle later...


        // Assign transaction number to commit
        int64_t txn = assignCommitTxn();
        
        // Update latest transaction number for all affected rows
        table->updateRowCommits(
            fragment->makeRowIterator(), txn);
        
        // Push the cells to the log queue
        queueCommit(fragment, txn);

        lock.unlock();

        // Defer response if necessary
        if(waitForSync)
            deferUntilDurable(cb, txn);
        else
            cb->done(txn);
    }
    catch(std::exception const & ex) {
        cb->error(ex);
    }
    catch(...) {
        cb->error(std::runtime_error("apply: unknown exception"));
    }
}

void TabletServer::sync_async(SyncCb * cb, int64_t waitForTxn)
{
    try {
        lock_t lock(serverMutex);

        // Clip waitForTxn to the last committed transaction
        if(waitForTxn > getLastCommitTxn())
            waitForTxn = getLastCommitTxn();

        // Get the last durable transaction;
        int64_t syncTxn = getLastDurableTxn();
        
        lock.unlock();

        // Defer response if necessary
        if(waitForTxn > syncTxn)
            deferUntilDurable(cb, waitForTxn);
        else
            cb->done(syncTxn);
    }
    catch(std::exception const & ex) {
        cb->error(ex);
    }
    catch(...) {
        cb->error(std::runtime_error("sync: unknown exception"));
    }
}
