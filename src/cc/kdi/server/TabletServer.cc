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
#include <kdi/server/ConfigReader.h>
#include <kdi/server/Table.h>
#include <kdi/server/Tablet.h>
#include <kdi/server/Fragment.h>
#include <kdi/server/Serializer.h>

#include <kdi/server/FragmentLoader.h>
#include <kdi/server/LogPlayer.h>
#include <kdi/server/ConfigWriter.h>

#include <kdi/server/name_util.h>
#include <kdi/server/errors.h>
#include <warp/call_or_die.h>
#include <warp/functional.h>
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

    struct Loading
    {
        TabletConfig const * config;
        std::vector<FragmentCPtr> frags;

        Loading() : config(0) {}
        Loading(TabletConfig const * config) :
            config(config) {}

        void addLoadedFragment(FragmentCPtr const & frag)
        {
            frags.push_back(frag);
        }

        bool operator<(Loading const & o) const
        {
            TabletConfig const & a = *config;
            TabletConfig const & b = *o.config;

            if(int c = warp::string_compare(a.log, b.log))
                return c < 0;

            if(int c = warp::string_compare(a.tableName, b.tableName))
                return c < 0;

            return a.rows.getUpperBound() < b.rows.getUpperBound();
        }

        struct LogNeq
        {
            strref_t log;
            LogNeq(strref_t log) : log(log) {}
            bool operator()(Loading const & x) const
            {
                return x.config->log != log;
            }
        };
    };

    class LoadCompleteCb
        : public Tablet::LoadedCb
    {
        size_t nPending;
        boost::mutex mutex;
        boost::condition cond;

    public:
        LoadCompleteCb() : nPending(0) {}
        
        void increment() { ++nPending; }

        void done()
        {
            lock_t lock(mutex);
            if(!--nPending)
                cond.notify_one();
        }

        void wait()
        {
            lock_t lock(mutex);
            while(nPending)
                cond.wait(lock);
        }
    };

}


//----------------------------------------------------------------------------
// TabletServer::ConfigsLoadedCb
//----------------------------------------------------------------------------
class TabletServer::ConfigsLoadedCb
    : public ConfigReader::ReadConfigsCb
{
    TabletServer * server;
    LoadCb * cb;

public:
    ConfigsLoadedCb(TabletServer * server, LoadCb * cb) :
        server(server),
        cb(cb)
    {
    }

    void done(std::vector<TabletConfig> const & configs)
    {
        try {
            server->loadTablets(configs);
            cb->done();
        }
        catch(std::exception const & ex) {
            cb->error(ex);
        }
        catch(...) {
            cb->error(std::runtime_error("load3: unknown exception"));
        }
        delete this;
    }

    void error(std::exception const & err)
    {
        cb->error(err);
        delete this;
    }
};


//----------------------------------------------------------------------------
// TabletServer::SchemasLoadedCb
//----------------------------------------------------------------------------
class TabletServer::SchemasLoadedCb
    : public ConfigReader::ReadSchemasCb
{
    TabletServer * server;
    LoadCb * cb;
    string_vec tablets;
    
public:
    SchemasLoadedCb(TabletServer * server, LoadCb * cb,
                    string_vec const & tablets) :
        server(server),
        cb(cb),
        tablets(tablets)
    {
    }

    void done(std::vector<TableSchema> const & schemas)
    {
        try {
            server->applySchemas(schemas);
            server->bits.configReader->readConfigs_async(
                new ConfigsLoadedCb(server, cb),
                tablets);
        }
        catch(std::exception const & ex) {
            cb->error(ex);
        }
        catch(...) {
            cb->error(std::runtime_error("load2: unknown exception"));
        }
        delete this;
    }

    void error(std::exception const & err)
    {
        cb->error(err);
        delete this;
    }
};


//----------------------------------------------------------------------------
// TabletServer
//----------------------------------------------------------------------------
TabletServer::TabletServer(Bits const & bits) :
    bits(bits)
{
    threads.create_thread(
        warp::callOrDie(
            boost::bind(&TabletServer::logLoop, this),
            "TabletServer::logLoop", true));

    threads.create_thread(
        warp::callOrDie(
            boost::bind(&TabletServer::serializeLoop, this),
            "TabletServer::serializeLoop", true));
}

TabletServer::~TabletServer()
{
    logQueue.cancelWaits();
    threads.join_all();

    for(table_map::const_iterator i = tableMap.begin();
        i != tableMap.end(); ++i)
    {
        delete i->second;
    }
}

void TabletServer::load_async(LoadCb * cb, string_vec const & tablets)
{
    /*
     *
     * - Validate input tablets:
     *   - Make sure each maps to a non-empty table name plus a last 
     *     row (character after table name is either ' ' or '!')
     *   - Filter out tablets that are already loaded
     * - Get table names for all remaining tablets
     * - Load any tables that aren't loaded yet
     *   - Load schemas from ConfigReader
     *   - Make sure loaded schemas are valid
     * - Load tablet configs from ConfigReader
     *   - (Optional) If config contains "location", make sure there 
     *     isn't a server actively serving the tablet at that location
     *   - If config contains "log", add (dir,tablet) to replay list
     *
     */

    try {

        string_vec neededTablets;
        string_vec neededTables;

        std::string tableName;
        warp::IntervalPoint<std::string> lastRow;

        lock_t serverLock(serverMutex);

        // Filter out tablets that are already loaded
        for(string_vec::const_iterator i = tablets.begin();
            i != tablets.end(); ++i)
        {
            decodeTabletName(*i, tableName, lastRow);
            table_map::const_iterator j = tableMap.find(tableName);
            if(j == tableMap.end())
            {
                neededTablets.push_back(*i);

                if(neededTables.empty() || neededTables.back() != tableName)
                    neededTables.push_back(tableName);

                continue;
            }

            Table * table = j->second;
            lock_t tableLock(table->tableMutex);
            Tablet * tablet = table->findTablet(lastRow);
            bool loading = tablet && tablet->isLoading();
            tableLock.unlock();

            if(!tablet)
                neededTablets.push_back(*i);
            else if(loading)
            {
                // We could install a callback and defer, but this is
                // a corner case we don't care much about.  Just
                // trigger an error for now so nothing gets confused
                // about what is loaded and when.
                throw std::runtime_error("already loading: " + *i);
            }
        }

        serverLock.unlock();

        // If we need to load tables, do so
        if(!neededTables.empty())
        {
            bits.configReader->readSchemas_async(
                new SchemasLoadedCb(this, cb, neededTablets),
                neededTables);
            return;
        }

        // If we need to load tablets, do so
        if(!neededTablets.empty())
        {
            bits.configReader->readConfigs_async(
                new ConfigsLoadedCb(this, cb),
                neededTablets);
            return;
        }

        // We have nothing to load
        cb->done();
    }
    catch(std::exception const & ex) {
        cb->error(ex);
    }
    catch(...) {
        cb->error(std::runtime_error("load: unknown exception"));
    }
}

void TabletServer::unload_async(UnloadCb * cb, string_vec const & tablets)
{
    try {
        // Unload phases:
        //  1) isolate tablets -- no more writes
        //  2) serialize any buffered mutations on unloading tablets
        //  3) update configs with final disk fragments;
        //     remove log dir and server columns
        //  4) wait for config sync
        //  5) disconnect readers
        //  6) drop tablet
        //  7) if table has no tablets, drop table too
        cb->error(std::runtime_error("not implemented"));

        std::string tableName;
        warp::IntervalPoint<std::string> lastRow;

        lock_t serverLock(serverMutex);

        for(string_vec::const_iterator i = tablets.begin();
            i != tablets.end(); ++i)
        {
            decodeTabletName(*i, tableName, lastRow);

            table_map::const_iterator j = tableMap.find(tableName);
            if(j == tableMap.end())
                continue;

            Table * table = j->second;
            lock_t tableLock(table->tableMutex);

            Tablet * tablet = table->findTablet(lastRow);
            if(!tablet)
                continue;

            // And... unimplemented...
        }

        // And... unimplemented...
        throw std::runtime_error("unload: not implemented");
    }
    catch(std::exception const & ex) {
        cb->error(ex);
    }
    catch(...) {
        cb->error(std::runtime_error("unload: unknown exception"));
    }
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

        // XXX: if the tablets are assigned to this server but still
        // in the process of being loaded, may want to defer until
        // they are ready.
        //
        // Actually, we can apply the change immediately, but the
        // sync/confirmation must be deferred until all affected
        // tablets have been fully loaded and their configs have been
        // updated to point to our log directory.
        //
        // This will keep us from blocking other writers if we get
        // changes for loading tablets.
        //
        // Rather than adding extra complexity to the sync part, we
        // can just defer the return of this call.  That also has the
        // nice effect of throttling writers that aren't waiting for a
        // sync.
        //
        // Also, we could consider the tablet "writable" after the log
        // is loaded but before all the fragments are loaded.
        //
        // Problem: if we're replaying the loading tablet's old log,
        // we need to make sure that this commit is applied after the
        // log replay.  The old logs could be interleaved with the new
        // apply.  Not quite sure how to handle log reloading anyway.
        // Maybe logged data needs to have a commit number associated
        // with it to allow out of order logging.
        //
        // Side question: when scanning, how do we scan only what is
        // durable?  This could be an important feature for the
        // fragment garbage collector.  Since we're just keeping a
        // mem-fragment vector, it wouldn't be too hard to track the
        // last durable fragment and only merge up to that point.  Or,
        // if each fragment has its commit number embedded, we could
        // scan with a max txn equal to the last durable txn.
        
        // Make sure tablets are loaded
        table->verifyTabletsLoaded(rows);

        // Make sure all cells map to loaded tablets and that the
        // transaction will apply.  The transaction can proceed only
        // if the last transaction in each row is less than or equal
        // to commitMaxTxn.
        if(txnCounter.getLastCommit() > commitMaxTxn)
            table->verifyCommitApplies(rows, commitMaxTxn);

        // XXX: How to throttle?
        //
        // We don't want to queue up too many heavy-weight objects
        // while waiting to commit to the log.  It would be bad to
        // make a full copy of the input data and queue it.  If we got
        // flooded with mutations we'd either: 1) use way too much
        // memory if we had a unbounded queue, or 2) block server
        // threads and starve readers if we have a bounded queue.
        //
        // Ideally, we'd leave mutations "on the wire" until we were
        // ready for them, while allowing read requests to flow
        // independently.
        //
        // If we use the Slice array mapping for the packed cell block
        // (and the backing buffer stays valid for the life of the AMD
        // invocation), then we can queue lightweight objects.  Ice
        // might be smart enough to do flow control for us in this
        // case.  I'm guessing it has a buffer per connection.  If we
        // tie up the buffer space, then the writing connection should
        // stall while allowing other connections to proceed.  Of
        // course interleaved reads on the same connection will
        // suffer, but that's not as big of a concern.
        //
        // Another potential solution: use different adapters for read
        // and write.  Or have a read/write adapter and a read-only
        // adapter.  To throttle we just block the caller, which will
        // eventually block all the threads on the write adapter's
        // pool.  Clients on the read-only adapter can continue.  I
        // like this solution as it becomes a detail of the RPC layer,
        // not the main server logic.
        //
        // Throttle later...


        // Assign transaction number to commit
        commit.txn = txnCounter.assignCommit();
        
        // Update latest transaction number for all affected rows
        table->updateRowCommits(rows, commit.txn);
        
        // Add the new fragment to the table's active list
        table->addMemoryFragment(commit.cells);

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

Table * TabletServer::findTable(strref_t tableName) const
{
    lock_t serverLock(serverMutex);
    table_map::const_iterator i = tableMap.find(
        tableName.toString());
    if(i != tableMap.end())
        return i->second;
    return 0;
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
            log.reset(bits.createNewLog());

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
            bits.workerPool->submit(durableCallbacks);

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

void TabletServer::serializeLoop()
{
    std::vector<Table*> tablesForSerializer;
    Serializer serializer;

    for(;;)
    {
        {
            lock_t serverLock(serverMutex);
            if(tablesForSerializer.empty()) 
            {
                for(table_map::const_iterator i = tableMap.begin();
                    i != tableMap.end(); ++i) 
                {
                    tablesForSerializer.push_back(i->second);
                }
            }
        }

        Table * t = tablesForSerializer.back();
        tablesForSerializer.pop_back();
        t->serialize(serializer);
    }
}

void TabletServer::applySchemas(std::vector<TableSchema> const & schemas)
{
    // For each schema:
    //   if the referenced table doesn't already exist, create it
    //   tell the table to update its schema

    lock_t serverLock(serverMutex);

    for(std::vector<TableSchema>::const_iterator i = schemas.begin();
        i != schemas.end(); ++i)
    {
        Table * & table = tableMap[i->tableName];
        if(!table)
            table = new Table;

        lock_t tableLock(table->tableMutex);
        table->applySchema(*i);
    }
}

// std::vector<TabletConfig::Fragment>
// topoMerge(std::vector<TabletConfig::Fragment> const & frags)
// {
//     std::vector<TabletConfig::Fragment> out;
// 
//     typedef std::vector<TabletConfig::Fragment> frag_vec;
//     typedef std::vector<std::string> str_vec;
//     typedef std::tr1::unordered_set<std::string> str_set;
//     typedef std::tr1::unordered_map<std::string, str_vec> ssv_map;
//     typedef std::tr1::unordered_map<std::string, str_set> sss_map;
// 
//     ssv_map colChains;
//     sss_map fragCols;
// 
//     for(frag_vec::const_iterator f = frags.begin();
//         f != frags.end(); ++f)
//     {
//         for(str_vec::const_iterator c = f->columns.begin();
//             c != f->columns.end(); ++c)
//         {
//             colChains[*c].push_back(f->tableName);
//             fragCols[f->tableName].insert(*c);
//         }
//     }
// 
//     //  colChain = {}  # col -> [fn1, fn2, ...]
//     //  fragCols = {}  # fn -> set(col1, col2, ..)
//     //
//     //  for fn, cols in config.fragments:
//     //     for col in cols.split():
//     //        colChain.setdefault(col,[]).append(fn)
//     //        fragCols.setdefault(fn,set()).add(col)
//     //  
//     //  for g in groups:
//     //     chains = [ colChain[c] for c in group.columns if c in colChain ]
//     //     g.frags = [ loadFrag(fn, fragCols[fn].intersection(group.columns))
//     //                 for fn in topoMerge(chains) ]
// 
// }

void TabletServer::loadTablets(std::vector<TabletConfig> const & configs)
{
    // Issues:
    //   - Tablets may be in mid-load from other callers
    //   - Tablets may already be loaded on this server
    //   - Tablets may have logs that need recovering
    //   - Mutations or scan requests may arrive for tablets that
    //     are currently loading.  These should be deferred.

    // Stages:
    //   - Lock server and walk through tablets
    //   - Partition into 4 disjoint sets:
    //     A: Already loaded
    //     B: Currently loading
    //     C: Not yet loading, needs log recovery
    //     D: Not yet loading, no log recovery needed
    //   - We don't care about set A
    //   - Ask for completion notifications on set B
    //   - Create inactive (loading) tablets for everything in (C+D)
    //   - Unlock server
    //   - For each tablet in C+D:
    //     - Load each fragment for the tablet
    //   - For each subset in C with the same log directory:
    //     - Replay log directory for ranges in the subset
    //   - For each tablet in C+D:
    //     - Save new tablet config
    //   - Sync configs
    //   - Lock server
    //   - For each tablet in C+D:
    //     - Activate tablet
    //     - Queue completion notifications
    //   - Unlock server
    //   - Issue queued completions
    //   - Complete after all notifications from B


    typedef std::vector<Loading> loading_vec;

    loading_vec loading;

    LoadCompleteCb loadCompleteCb;

    lock_t serverLock(serverMutex);

    for(std::vector<TabletConfig>::const_iterator i = configs.begin();
        i != configs.end(); ++i)
    {
        Table * table = getTable(i->tableName);
        lock_t tableLock(table->tableMutex);

        Tablet * tablet = table->findTablet(i->rows.getUpperBound());
        if(tablet)
        {
            if(tablet->isLoading())
            {
                loadCompleteCb.increment();
                tablet->deferUntilLoaded(&loadCompleteCb);
            }
            continue;
        }
        tablet = table->createTablet(i->rows);
        loading.push_back(Loading(&*i));
    }

    serverLock.unlock();

    std::sort(loading.begin(), loading.end());

    LogPlayer::filter_vec replayFilter;
    for(loading_vec::iterator last, first = loading.begin();
        first != loading.end(); first = last)
    {
        last = std::find_if(
            first+1, loading.end(),
            Loading::LogNeq(first->config->log));

        for(loading_vec::iterator i = first; i != last; ++i)
        {
            typedef std::vector<TabletConfig::Fragment> fvec;
            fvec const & frags = i->config->fragments;
            for(fvec::const_iterator f = frags.begin(); f != frags.end(); ++f)
            {
                FragmentCPtr frag = bits.fragmentLoader->load(
                    f->filename, f->columns);
                i->addLoadedFragment(frag);
            }
        }

        serverLock.lock();
        for(loading_vec::iterator i = first; i != last; ++i)
        {
            Table * table = getTable(i->config->tableName);
            lock_t tableLock(table->tableMutex);
            table->addLoadedFragments(i->config->rows, i->frags);
        }
        serverLock.unlock();

        if(first->config->log.empty())
            continue;

        for(loading_vec::const_iterator i = first;
            i != last; ++i)
        {
            replayFilter.push_back(
                std::make_pair(
                    i->config->tableName,
                    i->config->rows));
        }

        bits.logPlayer->replay(first->config->log, replayFilter);
        replayFilter.clear();
    }

    // XXX need to save config
    //for(loading_vec::const_iterator i = loading.begin();
    //    i != loading.end(); ++i)
    //{
    //    bits.configWriter->saveTablet(i->tablet);
    //}
    //bits.configWriter->sync();

        
    typedef std::vector<warp::Runnable *> callback_vec;
    callback_vec callbacks;

    serverLock.lock();

    for(loading_vec::const_iterator i = loading.begin();
        i != loading.end(); ++i)
    {
        Table * table = getTable(i->config->tableName);
        lock_t tableLock(table->tableMutex);
        Tablet * tablet = table->findTablet(i->config->rows.getUpperBound());
        warp::Runnable * loadCallback = tablet->finishLoading();
        if(loadCallback)
            callbacks.push_back(loadCallback);
    }

    serverLock.unlock();

    for(callback_vec::const_iterator i = callbacks.begin();
        i != callbacks.end(); ++i)
    {
        (*i)->run();
    }

    loadCompleteCb.wait();
}
