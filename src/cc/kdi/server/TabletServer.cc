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

#include <kdi/server/TabletServer.h>
#include <kdi/server/CellBuffer.h>
#include <kdi/server/Table.h>
#include <kdi/server/Fragment.h>
#include <kdi/server/errors.h>
#include <warp/call_or_die.h>
#include <boost/scoped_ptr.hpp>
#include <boost/bind.hpp>
#include <algorithm>

using namespace kdi;
using namespace kdi::server;

namespace {
    typedef boost::mutex::scoped_lock lock_t;


    class DeferredApplyCb
        : public TransactionCounter::DurableCb
    {
        TabletServer::ApplyCb * cb;

    public:
        explicit DeferredApplyCb(TabletServer::ApplyCb * cb) : cb(cb) {}
        void done(int64_t waitTxn, int64_t lastDurableTxn) {
            cb->done(waitTxn);
        }
    };

    class DeferredSyncCb
        : public TransactionCounter::DurableCb
    {
        TabletServer::SyncCb * cb;

    public:
        explicit DeferredSyncCb(TabletServer::SyncCb * cb) : cb(cb) {}
        void done(int64_t waitTxn, int64_t lastDurableTxn) {
            cb->done(lastDurableTxn);
        }
    };
}

//----------------------------------------------------------------------------
// TabletServer
//----------------------------------------------------------------------------
TabletServer::TabletServer(LogWriterFactory const & createNewLog,
                           warp::WorkerPool * workerPool) :
    createNewLog(createNewLog),
    workerPool(workerPool)
{
    threads.create_thread(
        warp::callOrDie(
            boost::bind(&TabletServer::logLoop, this),
            "TabletServer::logLoop", true));
}

TabletServer::~TabletServer()
{
    logQueue.cancelWaits();
    threads.join_all();
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
        Commit commit;
        commit.tableName.assign(tableName.begin(), tableName.end());
        commit.cells.reset(new CellBuffer(packedCells));

        // Get the rows from the commit
        std::vector<warp::StringRange> rows;
        commit.cells->getRows(rows);

        // Try to apply the commit
        lock_t serverLock(serverMutex);

        // Find the table
        Table * table = getTable(tableName);
        lock_t tableLock(table->tableMutex);

        // Note: if the tablets are assigned to this server but still
        // in the process of being loaded, may want to defer until
        // they are ready.
        
        // Make sure tablets are loaded
        table->verifyTabletsLoaded(rows);

        // Make sure all cells map to loaded tablets and that the
        // transaction will apply.  The transaction can proceed only
        // if the last transaction in each row is less than or equal
        // to commitMaxTxn.
        if(txnCounter.getLastCommit() > commitMaxTxn)
            table->verifyCommitApplies(rows, commitMaxTxn);

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
        commit.txn = txnCounter.assignCommit();
        
        // Update latest transaction number for all affected rows
        table->updateRowCommits(rows, commit.txn);
        
        // Add the new fragment to the table's active list
        table->addMemoryFragment(commit.cells, commit.cells->getDataSize());

        tableLock.unlock();

        // Push the cells to the log queue
        logPendingSz += commit.cells->getDataSize();
        logQueue.push(commit);

        // Defer response if necessary
        if(waitForSync)
        {
            txnCounter.deferUntilDurable(new DeferredApplyCb(cb), commit.txn);
            return;
        }

        serverLock.unlock();

        // Callback now
        cb->done(commit.txn);
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
        lock_t serverLock(serverMutex);

        // Clip waitForTxn to the last committed transaction
        if(waitForTxn > txnCounter.getLastCommit())
            waitForTxn = txnCounter.getLastCommit();
        
        // Get the last durable transaction
        int64_t syncTxn = txnCounter.getLastDurable();

        // Defer response if necessary
        if(waitForTxn > syncTxn)
        {
            txnCounter.deferUntilDurable(new DeferredSyncCb(cb), waitForTxn);
            return;
        }
        
        serverLock.unlock();

        // Callback now
        cb->done(syncTxn);
    }
    catch(std::exception const & ex) {
        cb->error(ex);
    }
    catch(...) {
        cb->error(std::runtime_error("sync: unknown exception"));
    }
}

Table * TabletServer::getTable(strref_t tableName) const
{
    table_map::const_iterator i = tableMap.find(
        tableName.toString());
    if(i == tableMap.end())
        throw TableNotLoadedError();
    return i->second;
}

void TabletServer::logLoop()
{
    typedef std::vector<Commit> commit_vec;
    typedef std::pair<std::string const *, Fragment const *> frag_pair;
    typedef std::vector<frag_pair> frag_vec;

    boost::scoped_ptr<LogWriter> log;

    commit_vec commits;
    frag_vec frags;
    Commit commit;
    while(logQueue.pop(commit))
    {
        // - throttle: block until logger.committedBufferSz < MAX_BUFFER_SIZE

        size_t commitSz = commit.cells->getDataSize();
        commits.push_back(commit);

        //- opt: while queue is not empty, gather more buffers
        while(logQueue.pop(commit, false))
        {
            commitSz += commit.cells->getDataSize();
            commits.push_back(commit);
        }

        // Start a new log if necessary
        if(!log)
            log.reset(createNewLog());

#if 0

        //- organize commit(s) by table, then by commit
        int64_t maxTxn = commits.back().txn;
        std::sort(commits.begin(), commits.end());

        // Process commits grouped by table
        for(commit_vec::const_iterator i = commits.begin();
            i != commits.end(); )
        {
            // Get range of commits for this table
            commit_vec::const_iterator i2 = std::find_if<commit_vec::const_iterator>(
                i+1, commits.end(), Commit::TableNeq(i->tableName));

            // Get the last transaction for the commit range
            int64_t txn = (i2-1)->txn;

            // If the range is more than one commit, merge all the
            // cell buffers into a single buffer
            CellBufferCPtr cells;
            if(i+1 == i2)
                cells = i->cells;
            else
                cells = mergeCommits(i, i2);

            // Write cells to log file with checksum
            log->writeCells(i->tableName, cells);

            // Move iterator to next table
            i = i2;
        }

#else

        // Feeling lazy: don't bother reordering or merging, just log
        // the commits in the order we received them
        int64_t maxTxn = commits.back().txn;
        for(commit_vec::const_iterator i = commits.begin();
            i != commits.end(); ++i)
        {
            log->writeCells(i->tableName, i->cells);
        }

#endif

        // Sync log to disk.  After this, commits up to maxTxn are
        // durable.
        log->sync();

        lock_t serverLock(serverMutex);

        // Update last durable txn
        warp::Runnable * durableCallbacks = txnCounter.setLastDurable(maxTxn);

        // Update queue sizes
        logPendingSz -= commitSz;

        serverLock.unlock();

        // Schedule deferred callbacks
        if(durableCallbacks)
            workerPool->submit(durableCallbacks);

        // Roll log if it's big enough
        size_t LOG_SIZE_LIMIT = 128u << 20;
        if(log->getDiskSize() >= LOG_SIZE_LIMIT)
        {
            log->close();
            log.reset();
        }

        // Clean up before next loop
        commits.clear();
        frags.clear();
        commit = Commit();
    }
}
