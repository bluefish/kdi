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
#include <kdi/server/SchemaReader.h>
#include <kdi/server/ConfigReader.h>
#include <kdi/server/Table.h>
#include <kdi/server/Tablet.h>
#include <kdi/server/TabletConfig.h>
#include <kdi/server/Fragment.h>
#include <kdi/server/DirectBlockCache.h>
#include <kdi/server/Serializer.h>
#include <kdi/server/Compactor.h>
#include <kdi/server/LogWriterFactory.h>
#include <kdi/server/LogWriter.h>

#include <kdi/server/FragmentLoader.h>
#include <kdi/server/FragmentWriterFactory.h>
#include <kdi/server/LogPlayer.h>
#include <kdi/server/ConfigWriter.h>
#include <kdi/server/PendingFile.h>

#include <kdi/server/name_util.h>
#include <kdi/server/errors.h>
#include <warp/PersistentWorker.h>
#include <warp/MultipleCallback.h>
#include <warp/call_or_die.h>
#include <warp/functional.h>
#include <warp/log.h>
#include <boost/scoped_ptr.hpp>
#include <boost/bind.hpp>
#include <algorithm>

using namespace kdi;
using namespace kdi::server;
using warp::log;

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

    class HoldPendingCb
        : public warp::Callback
    {
    public:
        explicit HoldPendingCb(PendingFileCPtr const & p) :
            p(p) {}
        
        virtual void done()
        {
            delete this;
        }

        virtual void error(std::exception const & err)
        {
            log("ERROR HoldPendingCb: %s", err.what());
            std::terminate();
        }

    private:        
        PendingFileCPtr p;
    };


    /// Get a TabletConfig handle for the given Tablet (which should
    /// be of the given TabletServer and Table).  The Table should be
    /// locked.
    TabletConfigCPtr getTabletConfig(TabletServer const * server,
                                     Table const * table,
                                     Tablet * tablet)
    {
        /// xxx This is nearly the stupidest way we could do this.

        std::vector<Tablet *> v;
        v.push_back(tablet);
        
        TabletConfigVecCPtr configs = table->getTabletConfigs(
            v,
            server->getLogDir(),
            server->getLocation());
        
        TabletConfigCPtr p(new TabletConfig((*configs)[0]));
        return p;
    }

    class SchemaLoader
        : public warp::Runnable
    {
    public:
        SchemaLoader(TabletServer::LoadSchemaCb * cb,
                     SchemaReader * reader,
                     std::string const & tableName) :
            cb(cb), reader(reader), tableName(tableName) {}

        virtual void run()
        {
            try {
                assert(reader);
                assert(cb);
                TableSchemaCPtr cfg = reader->readSchema(tableName);
                cb->done(cfg);
            }
            catch(std::exception const & err) { cb->error(err); }
            catch(...) {
                cb->error(std::runtime_error("SchemaLoader: unknown exception"));
            }
            delete this;
        }

    private:
        TabletServer::LoadSchemaCb * const cb;
        SchemaReader *               const reader;
        std::string                  const tableName;
    };

    class ConfigLoader
        : public warp::Runnable
    {
    public:
        ConfigLoader(TabletServer::LoadConfigCb * cb,
                     ConfigReader * reader,
                     std::string const & tabletName) :
            cb(cb), reader(reader), tabletName(tabletName) {}

        virtual void run()
        {
            assert(reader);
            assert(cb);

            try {
                TabletConfigCPtr cfg = reader->readConfig(tabletName);
                cb->done(cfg);
            }
            catch(std::exception const & err) { cb->error(err); }
            catch(...) {
                cb->error(std::runtime_error("ConfigLoader: unknown exception"));
            }
            delete this;
        }

    private:
        TabletServer::LoadConfigCb * const cb;
        ConfigReader *               const reader;
        std::string                  const tabletName;
    };

    class ConfigSaver
        : public warp::Runnable
    {
    public:
        ConfigSaver(warp::Callback * cb,
                    ConfigWriter * writer,
                    TabletConfigCPtr const & config) :
            cb(cb), writer(writer), config(config) {}

        virtual void run()
        {
            assert(cb);
            assert(writer);
            assert(config);

            try {
                writer->writeConfig(config);
                cb->done();
            }
            catch(std::exception const & err) { cb->error(err); }
            catch(...) {
                cb->error(std::runtime_error("ConfigSaver: unknown error"));
            }
            delete this;
        }

    private:
        warp::Callback *    const cb;
        ConfigWriter *      const writer;
        TabletConfigCPtr    const config;
    };

    class FragmentLoaderWork
        : public warp::Runnable
    {
    public:
        FragmentLoaderWork(TabletServer::LoadFragmentsCb * cb,
                           FragmentLoader * loader,
                           TabletConfigCPtr const & config) :
            cb(cb), loader(loader), config(config) {}

        virtual void run()
        {
            assert(cb);
            assert(loader);
            assert(config);

            try {
                std::vector<FragmentCPtr> loaded;

                typedef std::vector<TabletConfig::Fragment> fvec;
                fvec const & frags = config->fragments;

                for(fvec::const_iterator f = frags.begin();
                    f != frags.end(); ++f)
                {
                    FragmentCPtr frag = loader->load(f->filename);
                    frag = frag->getRestricted(f->families);
                    loaded.push_back(frag);
                }
                
                cb->done(loaded);
            }
            catch(std::exception const & err) { cb->error(err); }
            catch(...) {
                cb->error(std::runtime_error("FragmentLoaderWork: unknown error"));
            }
            delete this;
        }

    private:
        TabletServer::LoadFragmentsCb * const cb;
        FragmentLoader *                const loader;
        TabletConfigCPtr                const config;
    };

    class RetryApplyCb
        : public warp::Callback
    {
    public:
        RetryApplyCb(TabletServer::ApplyCb * cb,
                     TabletServer * server,
                     strref_t tableName,
                     CellBufferCPtr const & cells,
                     int64_t commitMaxTxn,
                     bool waitForSync) :
            cb(cb),
            server(server),
            tableName(tableName.begin(), tableName.end()),
            cells(cells),
            commitMaxTxn(commitMaxTxn),
            waitForSync(waitForSync),
            count(1)
        {
        }

    public:
        virtual void done()
        {
            maybeFinish();
        }

        virtual void error(std::exception const & err)
        {
            maybeFinish();
        }

        void incrementCount()
        {
            boost::mutex::scoped_lock lock(mutex);
            ++count;
        }

    private:
        void maybeFinish()
        {
            boost::mutex::scoped_lock lock(mutex);
            if(--count)
                return;

            // xxx -- this has the potential to deadlock for (very)
            // large cell buffers.  Also, it's inefficient.  This call
            // will allocate and verify a new buffer, even though we
            // already have one.  A better solution would be to make
            // an internal call that can reuse the shared buffer.

            server->apply_async(cb, tableName, cells->getPacked(),
                                commitMaxTxn, waitForSync);
            delete this;
        }

    private:
        TabletServer::ApplyCb * cb;
        TabletServer * server;
        std::string tableName;
        CellBufferCPtr cells;
        int64_t commitMaxTxn;
        bool waitForSync;

        boost::mutex mutex;
        size_t count;
    };

}

//----------------------------------------------------------------------------
// TabletServer::SchemaLoadedCb
//----------------------------------------------------------------------------
class TabletServer::SchemaLoadedCb
    : public TabletServer::LoadSchemaCb
{
    TabletServer * server;
    std::string tableName;
    
public:
    SchemaLoadedCb(TabletServer * server, std::string const & tableName) :
        server(server), tableName(tableName) {}

    void done(TableSchemaCPtr const & schema)
    {
        try {
            TabletServerLock serverLock(server);
            Table * table = server->getTable(tableName);
            TableLock tableLock(table);
            table->applySchema(*schema);
        }
        catch(std::exception const & ex) { fail(ex); }
        catch(...) { fail(std::runtime_error("unknown exception")); }
        delete this;
    }

    void error(std::exception const & err)
    {
        fail(err);
        delete this;
    }

private:
    void fail(std::exception const & err)
    {
        log("ERROR SchemaLoadedCb: %s", err.what());
    }
};

//----------------------------------------------------------------------------
// TabletServer::FragmentsLoadedCb
//----------------------------------------------------------------------------
class TabletServer::FragmentsLoadedCb
    : public TabletServer::LoadFragmentsCb
{
    TabletServer * server;
    std::string tabletName;

public:
    FragmentsLoadedCb(TabletServer * server, std::string const & tabletName) :
        server(server), tabletName(tabletName) {}

    void done(std::vector<FragmentCPtr> const & fragments)
    {
        try {
            std::string tableName;
            warp::IntervalPoint<std::string> lastRow;
            decodeTabletName(tabletName, tableName, lastRow);

            TabletServerLock serverLock(server);
            Table * table = server->getTable(tableName);
            TableLock tableLock(table);
            Tablet * tablet = table->getTablet(lastRow);

            assert(tablet->getState() == TABLET_FRAGMENTS_LOADING);

            // Apply the loaded Fragments
            tablet->onFragmentsLoaded(table->getSchema(), fragments);
            
            // Move to next load state
            warp::Runnable * callbacks = tablet->setState(TABLET_ACTIVE);

            tableLock.unlock();
            serverLock.unlock();

            // Issue load state callbacks, if any
            if(callbacks)
                callbacks->run();
        }
        catch(std::exception const & ex) { fail(ex); }
        catch(...) { fail(std::runtime_error("unknown exception")); }
        delete this;
    }

    void error(std::exception const & err)
    {
        fail(err);
        delete this;
    }

private:
    void fail(std::exception const & err)
    {
        log("ERROR FragmentsLoadedCb: %s", err.what());

        // xxx This should issue errors to all callbacks waiting for
        // the tablet to load and then delete the tablet.
        std::terminate();
    }
};

//----------------------------------------------------------------------------
// TabletServer::ConfigSavedCb
//----------------------------------------------------------------------------
class TabletServer::ConfigSavedCb
    : public warp::Callback
{
    TabletServer * server;
    TabletConfigCPtr config;

public:
    ConfigSavedCb(TabletServer * server, TabletConfigCPtr const & config) :
        server(server), config(config) {}

    void done()
    {
        try {
            TabletServerLock serverLock(server);
            Table * table = server->getTable(config->tableName);
            TableLock tableLock(table);
            Tablet * tablet = table->getTablet(config->rows.getUpperBound());

            assert(tablet->getState() == TABLET_CONFIG_SAVING);

            TabletState nextState;
            if(!config->fragments.empty())
            {
                // We need to load fragments
                nextState = TABLET_FRAGMENTS_LOADING;

                server->loadFragments_async(
                    new FragmentsLoadedCb(server, config->getTabletName()),
                    config);
            }
            else
            {
                // No fragments to load
                nextState = TABLET_ACTIVE;
            }

            // Move to next load state
            warp::Runnable * callbacks = tablet->setState(nextState);

            tableLock.unlock();
            serverLock.unlock();

            // Issue load state callbacks, if any
            if(callbacks)
                callbacks->run();
        }
        catch(std::exception const & ex) { fail(ex); }
        catch(...) { fail(std::runtime_error("unknown exception")); }
        delete this;
    }

    void error(std::exception const & err)
    {
        fail(err);
        delete this;
    }

private:
    void fail(std::exception const & err)
    {
        log("ERROR ConfigSavedCb: %s", err.what());

        // xxx This should issue errors to all callbacks waiting for
        // the tablet to load and then delete the tablet.
        std::terminate();
    }
};

//----------------------------------------------------------------------------
// TabletServer::LogReplayedCb
//----------------------------------------------------------------------------
class TabletServer::LogReplayedCb
    : public warp::Callback
{
    TabletServer * server;
    TabletConfigCPtr config;

public:
    LogReplayedCb(TabletServer * server, TabletConfigCPtr const & config) :
        server(server), config(config) {}

    void done()
    {
        try {
            TabletServerLock serverLock(server);
            Table * table = server->getTable(config->tableName);
            TableLock tableLock(table);
            Tablet * tablet = table->getTablet(config->rows.getUpperBound());

            assert(tablet->getState() == TABLET_LOG_REPLAYING);

            // Start the config save
            server->saveConfig_async(new ConfigSavedCb(server, config),
                                     getTabletConfig(server, table, tablet));

            // Move to next load state
            warp::Runnable * callbacks = tablet->setState(TABLET_CONFIG_SAVING);

            tableLock.unlock();
            serverLock.unlock();

            // Issue load state callbacks, if any
            if(callbacks)
                callbacks->run();
        }
        catch(std::exception const & ex) { fail(ex); }
        catch(...) { fail(std::runtime_error("unknown exception")); }
        delete this;
    }

    void error(std::exception const & err)
    {
        fail(err);
        delete this;
    }

private:
    void fail(std::exception const & err)
    {
        log("ERROR LogReplayedCb: %s", err.what());

        // xxx This should issue errors to all callbacks waiting for
        // the tablet to load and then delete the tablet.
        std::terminate();
    }
};

//----------------------------------------------------------------------------
// TabletServer::ConfigLoadedCb
//----------------------------------------------------------------------------
class TabletServer::ConfigLoadedCb
    : public TabletServer::LoadConfigCb
{
    TabletServer * server;
    std::string tabletName;

public:
    ConfigLoadedCb(TabletServer * server, std::string const & tabletName) :
        server(server), tabletName(tabletName) {}

    void done(TabletConfigCPtr const & config)
    {
        try {
            std::string tableName;
            warp::IntervalPoint<std::string> lastRow;
            decodeTabletName(tabletName, tableName, lastRow);
            
            TabletServerLock serverLock(server);
            Table * table = server->getTable(tableName);
            TableLock tableLock(table);
            Tablet * tablet = table->getTablet(lastRow);

            assert(tablet->getState() == TABLET_CONFIG_LOADING);

            // Get the Tablet to apply what it needs out of the config
            tablet->onConfigLoaded(config);

            // Move to the next load state
            TabletState nextState;
            if(!config->log.empty())
            {
                // The config specifies a log directory.  We need to
                // replay it.
                nextState = TABLET_LOG_REPLAYING;

                // Start the log replay
                server->replayLogs_async(new LogReplayedCb(server, config),
                                         tabletName, config->log);
            }
            else
            {
                // No log directory.  Move on to config save.
                nextState = TABLET_CONFIG_SAVING;

                // Start the config save
                server->saveConfig_async(new ConfigSavedCb(server, config),
                                         getTabletConfig(server, table, tablet));
            }

            // Move to next load state
            warp::Runnable * callbacks = tablet->setState(nextState);

            tableLock.unlock();
            serverLock.unlock();

            // Issue load state callbacks, if any
            if(callbacks)
                callbacks->run();
        }
        catch(std::exception const & ex) { fail(ex); }
        catch(...) { fail(std::runtime_error("unknown exception")); }
        delete this;
    }

    void error(std::exception const & err)
    {
        fail(err);
        delete this;
    }

private:
    void fail(std::exception const & err)
    {
        log("ERROR ConfigLoadedCb: %s", err.what());

        // xxx This should issue errors to all callbacks waiting for
        // the tablet to load and then delete the tablet.
        std::terminate();
    }
};


//----------------------------------------------------------------------------
// TabletServer::SWork
//----------------------------------------------------------------------------
class TabletServer::SWork
    : public Serializer::Work
{
public:
    SWork(TabletServer * server, std::string const & tableName,
          size_t schemaVersion, int groupIndex) :
        server(server),
        tableName(tableName),
        schemaVersion(schemaVersion),
        groupIndex(groupIndex)
    {
    }

    virtual void done(std::vector<std::string> const & rowCoverage,
                      PendingFileCPtr const & outFn)
    {
        TabletServerLock serverLock(server);

        // Make sure the table still exists
        Table * table = server->findTable(tableName);
        if(!table)
            return;

        // Lock table, release server
        TableLock tableLock(table);
        serverLock.unlock();

        // Make sure table schema hasn't changed
        if(table->getSchemaVersion() != schemaVersion)
            return;

        // Open new fragment
        FragmentCPtr newFrag = server->bits.fragmentLoader->load(outFn->getName());

        // Replace fragments
        std::vector<Tablet *> updatedTablets;
        table->replaceMemFragments(
            this->fragments,
            newFrag,
            groupIndex,
            rowCoverage,
            updatedTablets);

        // Notify event listeners
        assert(!rowCoverage.empty());
        table->issueFragmentEvent(
            warp::makeInterval(
                rowCoverage.front(),
                rowCoverage.back(),
                true, true),
            FET_FRAGMENTS_REPLACED);

        // Hold on to pending file reference until all configs are saved
        for(std::vector<Tablet *>::const_iterator i = updatedTablets.begin();
            i != updatedTablets.end(); ++i)
        {
            server->saveConfig_async(
                new HoldPendingCb(outFn),
                getTabletConfig(server, table, *i));
        }

        // Kick compactor
        server->wakeCompactor();
    }

private:
    TabletServer * server;
    std::string tableName;
    size_t schemaVersion;
    int groupIndex;
};

//----------------------------------------------------------------------------
// TabletServer::SInput
//----------------------------------------------------------------------------
class TabletServer::SInput
    : public Serializer::Input
{
public:
    explicit SInput(TabletServer * server) : server(server) {}

    virtual std::auto_ptr<Serializer::Work> getWork()
    {
        std::auto_ptr<Serializer::Work> ptr;

        if(!server->bits.fragmentFactory ||
           !server->bits.fragmentLoader)
        {
            return ptr;
        }

        TabletServerLock serverLock(server);

        // Find max mem-fragment group
        bool haveMax = false;
        table_map::const_iterator maxIt;
        int maxGroup = 0;
        float maxScore = 0;
        for(table_map::const_iterator i = server->tableMap.begin();
            i != server->tableMap.end(); ++i)
        {
            Table * table = i->second;
            TableLock tableLock(table);

            int nGroups = table->getSchema().groups.size();
            for(int j = 0; j < nGroups; ++j)
            {
                size_t memSize = table->getMemSize(j);
                //xxx int64_t txn = table->getEarliestMemCommit(j);
                size_t logSize = 0; // xxx: server->getLogSize(txn);
            
                float score = memSize + logSize * 0.1f;
            
                if(!haveMax || score > maxScore)
                {
                    haveMax = true;
                    maxIt = i;
                    maxGroup = j;
                    maxScore = score;
                }
            }
        }

        // Reject if the max is too small
        if(!haveMax || maxScore < 32e6f)
            return ptr;

        // Build new work unit
        Table * table = maxIt->second;
        TableLock tableLock(table);
        TableSchema const & schema = table->getSchema();
    
        ptr.reset(new SWork(server, maxIt->first, table->getSchemaVersion(), maxGroup));
        ptr->fragments = table->getMemFragments(maxGroup);
        ptr->predicate = schema.groups[maxGroup].getPredicate();
        ptr->output.reset(
            server->bits.fragmentFactory->start(
                schema, maxGroup).release());

        return ptr;
    }

private:    
    TabletServer * const server;
};

//----------------------------------------------------------------------------
// TabletServer::CWork
//----------------------------------------------------------------------------
class TabletServer::CWork
    : public Compactor::Work
{
public:
    CWork(TabletServer * server, size_t schemaVersion) :
        server(server), schemaVersion(schemaVersion) {}

    virtual void done(RangeOutputMap const & output)
    {
        TabletServerLock serverLock(server);

        // Make sure the table still exists
        Table * table = server->findTable(schema.tableName);
        if(!table)
            return;

        // Lock table, release server
        TableLock tableLock(table);
        serverLock.unlock();

        // Make sure table schema hasn't changed
        if(table->getSchemaVersion() != schemaVersion)
            return;

        // Replace fragments
        for(RangeOutputMap::const_iterator i = output.begin();
            i != output.end(); ++i)
        {
            RangeFragmentMap::const_iterator j =
                compactionSet.rangeMap.find(i->first);
            if(j == compactionSet.end())
                continue;

            FragmentCPtr newFragment;
            if(i->second)
            {
                newFragment = server->bits.fragmentLoader->load(
                    i->second->getName());
            }

            std::vector<Tablet *> updatedTablets;
            table->replaceDiskFragments(
                j->second, newFragment, groupIndex, i->first,
                updatedTablets);

            // Notify event listeners
            table->issueFragmentEvent(
                i->first,
                FET_FRAGMENTS_REPLACED);

            // Hold on to pending file reference until all configs are saved
            for(std::vector<Tablet *>::const_iterator j = updatedTablets.begin();
                j != updatedTablets.end(); ++j)
            {
                server->saveConfig_async(
                    new HoldPendingCb(i->second),
                    getTabletConfig(server, table, *j));
            }
        }
    }

private:
    TabletServer * const server;
    size_t schemaVersion;
};

//----------------------------------------------------------------------------
// TabletServer::CInput
//----------------------------------------------------------------------------
class TabletServer::CInput
    : public Compactor::Input
{
public:
    explicit CInput(TabletServer * server) : server(server) {}

    virtual std::auto_ptr<Compactor::Work> getWork()
    {
        std::auto_ptr<Compactor::Work> ptr;

        TabletServerLock serverLock(server);

        // Find longest disk-fragment chain
        size_t maxLen = 0;
        table_map::const_iterator maxIt;
        int maxGroup = -1;
        for(table_map::const_iterator i = server->tableMap.begin();
            i != server->tableMap.end(); ++i)
        {
            Table * table = i->second;
            TableLock tableLock(table);

            int nGroups = table->getSchema().groups.size();
            for(int j = 0; j < nGroups; ++j)
            {
                size_t len = table->getMaxDiskChainLength(j);
                if(len > maxLen)
                {
                    maxLen = len;
                    maxIt = i;
                    maxGroup = j;
                }
            }
        }

        // Don't bother?
        if(maxLen < 5)
            return ptr;

        // Build a work unit
        Table * table = maxIt->second;
        TableLock tableLock(table);
        
        ptr.reset(new CWork(server, table->getSchemaVersion()));
        ptr->schema = table->getSchema();
        ptr->groupIndex = maxGroup;
        table->getCompactionSet(maxGroup, ptr->compactionSet);

        return ptr;
    }

private:
    TabletServer * const server;
};

//----------------------------------------------------------------------------
// TabletServer::Workers
//----------------------------------------------------------------------------
class TabletServer::Workers
{
public:
    SInput serializerInput;
    CInput compactorInput;
    DirectBlockCache compactorCache;
    Serializer serializer;
    Compactor compactor;
    warp::WorkerPool configWorker;
    warp::WorkerPool fragmentWorkers;

    Workers(TabletServer * server) :
        serializerInput(server),
        compactorInput(server),
        serializer(&serializerInput),
        compactor(&compactorInput,
                  server->bits.fragmentFactory,
                  &compactorCache),
        configWorker(1, "Config", true),
        fragmentWorkers(2, "Fragment", true)
    {
    }

    void start()
    {
        serializer.start();
        compactor.start();
    }

    void stop()
    {
        compactor.stop();
        serializer.stop();
    }
};

//----------------------------------------------------------------------------
// TabletServer
//----------------------------------------------------------------------------
TabletServer::TabletServer(Bits const & bits) :
    bits(bits),
    cellAllocator(bits.maxBufferSz),
    workers(new Workers(this))
{
    threads.create_thread(
        warp::callOrDie(
            boost::bind(&TabletServer::logLoop, this),
            "TabletServer::logLoop", true));

    workers->start();
}

TabletServer::~TabletServer()
{
    workers->stop();

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
    warp::MultipleCallback * mcb = new warp::MultipleCallback(cb);
    try {
        std::string tableName;
        warp::IntervalPoint<std::string> lastRow;

        TabletServerLock serverLock(this);

        // Walk list of tablets, start loads where needed and defer
        // return until all tablets are active.
        for(string_vec::const_iterator i = tablets.begin();
            i != tablets.end(); ++i)
        {
            std::string const & tabletName = *i;
            decodeTabletName(tabletName, tableName, lastRow);

            // Find the table (or create it)
            Table * & table = tableMap[tableName];
            if(!table)
            {
                // Create a new Table with default schema
                table = new Table(tableName);

                // Initiate a schema load for the Table
                loadSchema_async(new SchemaLoadedCb(this, tableName), tableName);
            }

            // Lock the table and find the tablet (or create it)
            TableLock tableLock(table);
            Tablet * tablet = table->findTablet(lastRow);
            if(!tablet)
            {
                // Create a new Tablet
                tablet = table->createTablet(lastRow);

                // Initiate a config load for the Tablet
                loadConfig_async(new ConfigLoadedCb(this, tabletName), tabletName);
            }

            if(tablet->getState() < TABLET_ACTIVE)
            {
                // Tablet is still loading.  Defer until it finishes.
                mcb->incrementCount();
                tablet->deferUntilState(mcb, TABLET_ACTIVE);
            }
            else if(tablet->getState() > TABLET_ACTIVE)
            {
                // xxx - could defer and retry for unloading tablets

                // Tablet in an unloading or error state.
                using namespace ex;
                raise<RuntimeError>(
                    "Tablet is in %s state: %s",
                    tablet->getState() == TABLET_ERROR ? "error" : "unloading",
                    warp::reprString(tabletName));
            }
        }

        serverLock.unlock();

        // We're done here, let the callbacks come in
        mcb->done();
    }
    catch(std::exception const & ex) {
        mcb->error(ex);
    }
    catch(...) {
        mcb->error(std::runtime_error("load1: unknown exception"));
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

        TabletServerLock serverLock(this);

        for(string_vec::const_iterator i = tablets.begin();
            i != tablets.end(); ++i)
        {
            decodeTabletName(*i, tableName, lastRow);

            table_map::const_iterator j = tableMap.find(tableName);
            if(j == tableMap.end())
                continue;

            Table * table = j->second;
            TableLock tableLock(table);

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
        tableName.assignTo(commit.tableName);

        // Allocate buffer space.  This may block, and acts as a
        // throttling point to avoid write-overload.
        commit.cells = cellAllocator.allocate(packedCells);

        // Get the rows and column families from the commit
        std::vector<warp::StringRange> rows;
        std::vector<std::string> families;
        commit.cells->getRows(rows);
        commit.cells->getColumnFamilies(families);

        // Try to apply the commit
        TabletServerLock serverLock(this);

        // Find the table
        Table * table = getTable(tableName);
        TableLock tableLock(table);
        
        // Make sure tablets are loaded.
        std::vector<Tablet *> loadingTablets;
        table->verifyTabletsLoaded(rows, loadingTablets);
        if(!loadingTablets.empty())
        {
            // Some Tablets are still loading.  We must defer until
            // all Tablets are loaded.
            RetryApplyCb * rcb = new RetryApplyCb(
                cb,
                this,
                tableName,
                commit.cells,
                commitMaxTxn,
                waitForSync);

            for(std::vector<Tablet *>::const_iterator i =
                    loadingTablets.begin();
                i != loadingTablets.end(); ++i)
            {
                rcb->incrementCount();
                (*i)->deferUntilState(rcb, TABLET_FRAGMENTS_LOADING);
            }

            rcb->done();
            return;
        }

        // Make sure columns fit in with current schema
        table->verifyColumnFamilies(families);

        // Make sure all cells map to loaded tablets and that the
        // transaction will apply.  The transaction can proceed only
        // if the last transaction in each row is less than or equal
        // to commitMaxTxn.
        if(txnCounter.getLastCommit() > commitMaxTxn)
            table->verifyCommitApplies(rows, commitMaxTxn);

        // Assign transaction number to commit
        commit.txn = txnCounter.assignCommit();
        
        // Update latest transaction number for all affected rows
        table->updateRowCommits(rows, commit.txn);
        
        // Add the new fragment to the table's active list
        table->addMemoryFragment(commit.cells);

        // Notify event listeners
        assert(!rows.empty());
        table->issueFragmentEvent(
            warp::makeInterval(
                rows.front().toString(),
                rows.back().toString(),
                true, true),
            FET_NEW_FRAGMENT);

        tableLock.unlock();

        // Signal the serializer
        wakeSerializer();

        // Push the cells to the log queue
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
        TabletServerLock serverLock(this);

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

void TabletServer::loadSchema_async(
    LoadSchemaCb * cb, std::string const & tableName)
{
    try {
        workers->configWorker.submit(
            new SchemaLoader(
                cb,
                bits.schemaReader,
                tableName));
    }
    catch(std::exception const & ex) { cb->error(ex); }
    catch(...) {
        cb->error(std::runtime_error("loadSchema: unknown exception"));
    }
}

void TabletServer::loadConfig_async(
    LoadConfigCb * cb, std::string const & tabletName)
{
    try {
        workers->configWorker.submit(
            new ConfigLoader(
                cb,
                bits.configReader,
                tabletName));
    }
    catch(std::exception const & ex) { cb->error(ex); }
    catch(...) {
        cb->error(std::runtime_error("loadConfig: unknown exception"));
    }
}

void TabletServer::replayLogs_async(
    warp::Callback * cb, std::string const & tabletName,
    std::string const & logDir)
{
    try {
        EX_UNIMPLEMENTED_FUNCTION;
    }
    catch(std::exception const & ex) { cb->error(ex); }
    catch(...) {
        cb->error(std::runtime_error("replayLogs: unknown exception"));
    }
}

void TabletServer::saveConfig_async(
    warp::Callback * cb, TabletConfigCPtr const & config)
{
    try {
        workers->configWorker.submit(
            new ConfigSaver(
                cb,
                bits.configWriter,
                config));
    }
    catch(std::exception const & ex) { cb->error(ex); }
    catch(...) {
        cb->error(std::runtime_error("saveConfig: unknown exception"));
    }
}

void TabletServer::loadFragments_async(
    LoadFragmentsCb * cb, TabletConfigCPtr const & config)
{
    try {
        workers->fragmentWorkers.submit(
            new FragmentLoaderWork(
                cb,
                bits.fragmentLoader,
                config));
    }
    catch(std::exception const & ex) { cb->error(ex); }
    catch(...) {
        cb->error(std::runtime_error("loadFragments: unknown exception"));
    }
}

Table * TabletServer::findTable(strref_t tableName) const
{
    table_map::const_iterator i = tableMap.find(tableName.toString());
    if(i != tableMap.end())
        return i->second;
    return 0;
}

Table * TabletServer::getTable(strref_t tableName) const
{
    Table * table = findTable(tableName);
    if(!table)
        throw TableNotLoadedError();
    return table;
}

void TabletServer::wakeSerializer()
{
    workers->serializer.wake();
}

void TabletServer::wakeCompactor()
{
    workers->compactor.wake();
}

void TabletServer::logLoop()
{
    size_t const GROUP_COMMIT_LIMIT = size_t(4) << 20;
    size_t const LOG_SIZE_LIMIT = size_t(128) << 20;

    boost::scoped_ptr<LogWriter> log;

    Commit commit;
    while(logQueue.pop(commit))
    {
        // Start a new log if necessary
        if(!log && bits.logFactory)
            log.reset(bits.logFactory->start().release());

        size_t commitSz = 0;
        int64_t maxTxn;

        // We got a commit to log.  Keep logging until the next queue
        // fetch would block or we've written a significant amount of
        // data.
        do {
            commitSz += commit.cells->getDataSize();
            maxTxn = commit.txn;

            // Log the commit to disk
            if(log)
                log->writeCells(
                    commit.tableName, commit.cells->getPacked());

            // Release the cells
            commit.cells.reset();

        } while( commitSz < GROUP_COMMIT_LIMIT &&
                 logQueue.pop(commit, false) );

        // Sync log to disk.  After this, commits up to maxTxn are
        // durable.
        if(log)
            log->sync();

        TabletServerLock serverLock(this);

        // Update last durable txn
        warp::Runnable * durableCallbacks = txnCounter.setLastDurable(maxTxn);

        serverLock.unlock();

        // Schedule deferred callbacks
        if(durableCallbacks)
            bits.workerPool->submit(durableCallbacks);

        // Roll log if it's big enough
        if(log && log->getDiskSize() >= LOG_SIZE_LIMIT)
        {
            std::string logFn = log->finish();
            log.reset();

            /// XXX track the new log file so we can clean it up when
            /// it is no longer needed
            //if(bits.logTracker)
            //    bits.logTracker->addLogFile(logFn, maxTxn);
        }
    }
}
