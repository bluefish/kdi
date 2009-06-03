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
        Table * table = server->findTableLocked(tableName);
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

        /// Hold on to pending file reference until all configs are saved
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
        Table * table = server->findTableLocked(schema.tableName);
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
            if(!i->second)
            {
                newFragment = server->bits.fragmentLoader->load(
                    i->second->getName());
            }

            std::vector<Tablet *> updatedTablets;
            table->replaceDiskFragments(
                j->second, newFragment, groupIndex, i->first,
                updatedTablets);

            /// Hold on to pending file reference until all configs are saved
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
        commit.cells.reset(new CellBuffer(packedCells));

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

        // Make sure columns fit in with current schema
        table->verifyColumnFamilies(families);

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

        // Signal the serializer
        wakeSerializer();

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
    TabletServerLock serverLock(this);
    return findTableLocked(tableName);
}

Table * TabletServer::findTableLocked(strref_t tableName) const
{
    table_map::const_iterator i = tableMap.find(
        tableName.toString());
    if(i != tableMap.end())
        return i->second;
    return 0;
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
        // xxx ?? throttle: block until logger.committedBufferSz < MAX_BUFFER_SIZE

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

        // Update queue sizes
        logPendingSz -= commitSz;

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

#if 0

// This function has been replaced by the async loading chain.  That
// chain still has some holes in it.  This is reference until those
// get filled.

namespace {
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

    TabletServerLock serverLock(this);

    for(std::vector<TabletConfig>::const_iterator i = configs.begin();
        i != configs.end(); ++i)
    {
        Table * table = getTable(i->tableName);
        TableLock tableLock(table);

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
        tablet = table->createTablet(i->rows.getUpperBound());
        tablet->setLowerBound(i->rows.getLowerBound());
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
                FragmentCPtr frag = bits.fragmentLoader->load(f->filename);
                frag = frag->getRestricted(f->families);
                i->addLoadedFragment(frag);
            }
        }

        serverLock.lock();
        for(loading_vec::iterator i = first; i != last; ++i)
        {
            Table * table = getTable(i->config->tableName);
            TableLock tableLock(table);
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

    // Save configs for all loaded tablets
    {
        // Init to size of all loaded tablets
        TabletConfigVecPtr configs(new TabletConfigVec(loading.size()));
        for(size_t i = 0; i < configs->size(); ++i)
        {
            // Set log and location to reference this server
            (*configs)[i].log = bits.serverLogDir;
            (*configs)[i].location = bits.serverLocation;

            // Set name and rows from loaded config (assuming no
            // splits possible?)
            (*configs)[i].tableName = loading[i].config->tableName;
            (*configs)[i].rows = loading[i].config->rows;
        }

        // Get Fragment lists for each Tablet
        serverLock.lock();
        for(size_t i = 0; i < configs->size(); ++i)
        {
            Table * table = getTable((*configs)[i].tableName);
            TableLock tableLock(table);
            Tablet * tablet = table->findTablet((*configs)[i].rows.getUpperBound());
            tablet->getConfigFragments((*configs)[i]);
        }
        serverLock.unlock();

        warp::WaitCallback cb;
        saveConfigs_async(&cb, configs);
        cb.wait();
    }
        
    typedef std::vector<warp::Runnable *> callback_vec;
    callback_vec callbacks;

    serverLock.lock();

    for(loading_vec::const_iterator i = loading.begin();
        i != loading.end(); ++i)
    {
        Table * table = getTable(i->config->tableName);
        TableLock tableLock(table);
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

#endif
