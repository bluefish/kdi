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
#include <warp/fs.h>
#include <warp/timer.h>
#include <boost/scoped_ptr.hpp>
#include <boost/bind.hpp>
#include <algorithm>

using namespace kdi;
using namespace kdi::server;
using warp::log;

namespace {

    void deleteLogFile(std::string const & fn)
    {
        log("Deleting log file: %s", fn);
        warp::fs::remove(fn);
    }
    
    class DeleteLogFileCb
        : public TransactionCounter::TransactionCb
    {
        std::string fn;

    public:
        explicit DeleteLogFileCb(std::string const & fn) : fn(fn) {}
        void done(int64_t waitTxn, int64_t lastDurableTxn,
                  int64_t lastStableTxn)
        {
            deleteLogFile(fn);
            delete this;
        }
    };

    class DeferredApplyCb
        : public TransactionCounter::TransactionCb
    {
        TabletServer::ApplyCb * cb;

    public:
        explicit DeferredApplyCb(TabletServer::ApplyCb * cb) : cb(cb) {}
        void done(int64_t waitTxn, int64_t lastDurableTxn,
                  int64_t lastStableTxn)
        {
            cb->done(waitTxn);
            delete this;
        }
    };

    class DeferredSyncCb
        : public TransactionCounter::TransactionCb
    {
        TabletServer::SyncCb * cb;

    public:
        explicit DeferredSyncCb(TabletServer::SyncCb * cb) : cb(cb) {}
        void done(int64_t waitTxn, int64_t lastDurableTxn,
                  int64_t lastStableTxn)
        {
            cb->done(lastDurableTxn);
            delete this;
        }
    };

    class CompactionStableCb
        : public warp::Callback
    {
    public:
        CompactionStableCb(TabletServer * server,
                           std::vector<FragmentCPtr> const & oldFragments,
                           PendingFileCPtr const & pendingFile) :
            server(server),
            oldFragments(oldFragments),
            pendingFile(pendingFile)
        {
        }

        virtual void done() throw()
        {
            server->onCompactionStable(oldFragments);
            delete this;
        }

        virtual void error(std::exception const & err) throw()
        {
            log("ERROR CompactionStableCb: %s", err.what());
            std::terminate();
            delete this;
        }

    private:
        TabletServer * server;
        std::vector<FragmentCPtr> oldFragments;
        PendingFileCPtr pendingFile;
    };

    class SerializationStableCb
        : public warp::Callback
    {
    public:
        SerializationStableCb(TabletServer * server,
                              int64_t pendingTxn,
                              PendingFileCPtr const & pendingFile) :
            server(server),
            pendingTxn(pendingTxn),
            pendingFile(pendingFile) {}

        virtual void done() throw()
        {
            server->onSerializationStable(pendingTxn);
            delete this;
        }

        virtual void error(std::exception const & err) throw()
        {
            log("ERROR SerializationStableCb: %s", err.what());
            std::terminate();
            delete this;
        }

    private:
        TabletServer * server;
        int64_t pendingTxn;
        PendingFileCPtr pendingFile;
    };


    /// Get a TabletConfig handle for the given Tablet (which should
    /// be of the given TabletServer and Table).  The Table should be
    /// locked.
    TabletConfigCPtr getTabletConfig(TabletServer const * server,
                                     Table const * table,
                                     Tablet * tablet)
    {
        assert(tablet->getState() >= TABLET_CONFIG_SAVING &&
               tablet->getState() <= TABLET_ACTIVE);

        TabletConfigPtr p(new TabletConfig);

        p->tableName = table->getSchema().tableName;
        p->rows = tablet->getRows();
        tablet->getConfigFragments(*p);
        p->log = server->getLogDir();
        p->location = server->getLocation();

        return p;
    }

    class DeferredConfigSaveCb
        : public warp::Callback
    {
    public:
        DeferredConfigSaveCb(warp::Callback * cb,
                             TabletServer * server,
                             std::string const & tabletName) :
            cb(cb),
            server(server),
            tabletName(tabletName)
        {
        }

        virtual void done() throw()
        {
            try {
                std::string tableName;
                warp::IntervalPoint<std::string> lastRow;
                decodeTabletName(tabletName, tableName, lastRow);

                TabletServerLock serverLock(server);
                Table * table = server->getTable(tableName);
                TableLock tableLock(table);
                Tablet * tablet = table->getTablet(lastRow);

                server->saveConfig_async(
                    cb, getTabletConfig(server, table, tablet));
            }
            catch(std::exception const & err) { fail(err); }
            catch(...) { fail(std::runtime_error("unknown exception")); }
            delete this;
        }

        virtual void error(std::exception const & err) throw()
        {
            fail(err);
            delete this;
        }

    private:
        void fail(std::exception const & err)
        {
            log("ERROR DeferredConfigSaveCb: %s", err.what());
            cb->error(err);
        }

    private:
        warp::Callback * const cb;
        TabletServer *   const server;
        std::string      const tabletName;
    };

    // Schedule a Tablet for a config save, holding the pending file
    // until the config is durable.  If the Tablet is currently still
    // loading, the config save will be deferred until the Tablet is
    // allowed to save.
    void scheduleConfigSave(warp::Callback * cb,
                            TabletServer * server,
                            Table * table,
                            Tablet * tablet)
    {
        if(tablet->getState() < TABLET_CONFIG_SAVING)
        {
            // Defer save until later
            tablet->deferUntilState(
                new DeferredConfigSaveCb(
                    cb,
                    server,
                    encodeTabletName(
                        table->getSchema().tableName,
                        tablet->getLastRow())),
                TABLET_CONFIG_SAVING);
        }
        else
        {
            // Save now
            server->saveConfig_async(
                cb, getTabletConfig(server, table, tablet));
        }
    }


    class SchemaLoader
        : public warp::Runnable
    {
    public:
        SchemaLoader(TabletServer::LoadSchemaCb * cb,
                     SchemaReader * reader,
                     std::string const & tableName) :
            cb(cb), reader(reader), tableName(tableName) {}

        virtual void run() throw()
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

        virtual void run() throw()
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

                
    class LogReplayer
        : public warp::Runnable
    {
    public:
        LogReplayer(warp::Callback * cb,
                    TabletServer * server,
                    std::string const & logDir,
                    std::string const & tableName,
                    warp::Interval<std::string> const & rows) :
            cb(cb),
            server(server),
            logDir(logDir),
            tableName(tableName),
            rows(rows)
        {
        }

        virtual void run() throw()
        {
            log("ERROR ignoring log replay: logDir=%s, table=%s, rows=%s",
                logDir, tableName, rows);
            cb->done();
        }

    private:
        warp::Callback *              const cb;
        TabletServer *                const server;
        std::string                   const logDir;
        std::string                   const tableName;
        warp::Interval<std::string>   const rows;
    };


    class ConfigSaver
        : public warp::Runnable
    {
    public:
        ConfigSaver(warp::Callback * cb,
                    ConfigWriter * writer,
                    TabletConfigCPtr const & config) :
            cb(cb), writer(writer), config(config) {}

        virtual void run() throw()
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

        virtual void run() throw()
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
        virtual void done() throw()
        {
            maybeFinish();
        }

        virtual void error(std::exception const & err) throw()
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

    void done(TableSchemaCPtr const & schema) throw()
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

    void error(std::exception const & err) throw()
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

    void done(std::vector<FragmentCPtr> const & fragments) throw()
    {
        warp::Runnable * callbacks = 0;

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
            callbacks = tablet->setState(TABLET_ACTIVE);
        }
        catch(std::exception const & ex) { fail(ex); }
        catch(...) { fail(std::runtime_error("unknown exception")); }

        // Issue load state callbacks, if any
        if(callbacks)
            callbacks->run();

        delete this;
    }

    void error(std::exception const & err) throw()
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

    void done() throw()
    {
        warp::Runnable * callbacks = 0;

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
            callbacks = tablet->setState(nextState);
        }
        catch(std::exception const & ex) { fail(ex); }
        catch(...) { fail(std::runtime_error("unknown exception")); }

        // Issue load state callbacks, if any
        if(callbacks)
            callbacks->run();

        delete this;
    }

    void error(std::exception const & err) throw()
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

    void done() throw()
    {
        warp::Runnable * callbacks = 0;

        try {
            TabletServerLock serverLock(server);
            Table * table = server->getTable(config->tableName);
            TableLock tableLock(table);
            Tablet * tablet = table->getTablet(config->rows.getUpperBound());

            assert(tablet->getState() == TABLET_LOG_REPLAYING);

            // Move to next load state
            callbacks = tablet->setState(TABLET_CONFIG_SAVING);

            // Start the config save
            server->saveConfig_async(new ConfigSavedCb(server, config),
                                     getTabletConfig(server, table, tablet));
        }
        catch(std::exception const & ex) { fail(ex); }
        catch(...) { fail(std::runtime_error("unknown exception")); }

        // Issue load state callbacks, if any
        if(callbacks)
            callbacks->run();

        delete this;
    }

    void error(std::exception const & err) throw()
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

    void done(TabletConfigCPtr const & config) throw()
    {
        warp::Runnable * callbacks = 0;

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
            if(!config->log.empty())
            {
                // The config specifies a log directory.  We need to
                // replay it.
                callbacks = tablet->setState(TABLET_LOG_REPLAYING);

                // Start the log replay
                server->replayLogs_async(new LogReplayedCb(server, config),
                                         config->log, tableName,
                                         tablet->getRows());
            }
            else
            {
                // No log directory.  Move on to config save.
                callbacks = tablet->setState(TABLET_CONFIG_SAVING);

                // Start the config save
                server->saveConfig_async(new ConfigSavedCb(server, config),
                                         getTabletConfig(server, table, tablet));
            }
        }
        catch(std::exception const & ex) { fail(ex); }
        catch(...) { fail(std::runtime_error("unknown exception")); }

        // Issue load state callbacks, if any
        if(callbacks)
            callbacks->run();

        delete this;
    }

    void error(std::exception const & err) throw()
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
        // Open new fragment
        FragmentCPtr newFrag = server->bits.fragmentLoader->load(outFn->getName());
        newFrag = server->localGc.wrapLocalFragment(newFrag);

        TabletServerLock serverLock(server);
        Table * table = server->findTable(tableName);
        if(!table)
            return;
        TableLock tableLock(table);
        if(table->getSchemaVersion() != schemaVersion)
            return;

        int64_t pendingTxn = table->getEarliestMemCommit() - 1;
        server->addPendingSerialization(pendingTxn);

        serverLock.unlock();

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

        tableLock.unlock();

        SerializationStableCb * cb =
            new SerializationStableCb(server, pendingTxn, outFn);
        warp::MultipleCallback * mcb = new warp::MultipleCallback(cb);

        // Hold on to pending file reference until all configs are saved
        for(std::vector<Tablet *>::const_iterator i = updatedTablets.begin();
            i != updatedTablets.end(); ++i)
        {
            mcb->incrementCount();
            scheduleConfigSave(mcb, server, table, *i);
        }

        mcb->done();

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
        table->getMemFragments(maxGroup, ptr->fragments);
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

        warp::MultipleCallback * mcb = new warp::MultipleCallback;

        // Replace fragments
        for(RangeOutputMap::const_iterator i = output.begin();
            i != output.end(); ++i)
        {
            warp::Interval<std::string> const & rows = i->first;
            PendingFileCPtr const & pendingOutput = i->second;

            RangeFragmentMap::const_iterator j =
                compactionSet.rangeMap.find(rows);
            if(j == compactionSet.end())
                continue;

            std::vector<FragmentCPtr> const & oldFragments = j->second;

            FragmentCPtr newFragment;
            if(pendingOutput)
            {
                newFragment = server->bits.fragmentLoader->load(
                    pendingOutput->getName());
                newFragment = server->localGc.wrapLocalFragment(newFragment);
            }

            std::vector<Tablet *> updatedTablets;
            table->replaceDiskFragments(
                oldFragments, newFragment, groupIndex, rows,
                updatedTablets);

            // Notify event listeners
            table->issueFragmentEvent(
                rows,
                FET_FRAGMENTS_REPLACED);

            mcb->addCallback(
                new CompactionStableCb(
                    server, oldFragments, pendingOutput));

            // Hold on to pending file reference until all configs are saved
            for(std::vector<Tablet *>::const_iterator j = updatedTablets.begin();
                j != updatedTablets.end(); ++j)
            {
                mcb->incrementCount();
                scheduleConfigSave(mcb, server, table, *j);
            }
        }

        tableLock.unlock();
        mcb->done();
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
    localGc(bits.fragmentRemover),
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
    cb->error(std::runtime_error("unload not implemented"));
    return;

    warp::MultipleCallback * mcb = new warp::MultipleCallback(cb);
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

        std::string tableName;
        warp::IntervalPoint<std::string> lastRow;

        TabletServerLock serverLock(this);

        for(string_vec::const_iterator i = tablets.begin();
            i != tablets.end(); ++i)
        {
            decodeTabletName(*i, tableName, lastRow);

            Table * table = findTable(tableName);
            if(!table)
            {
                // Table doesn't exist, nothing to do
                continue;
            }
            TableLock tableLock(table);
            Tablet * tablet = table->findTablet(lastRow);
            if(!tablet)
            {
                // Tablet doesn't exist, nothing to do
                continue;
            }

            // We're return after all tablets get to the unloaded
            // state.
            mcb->incrementCount();
            tablet->deferUntilState(mcb, TABLET_UNLOADED);

            if(tablet->getState() < TABLET_ACTIVE)
            {
                // Tablet is still loading: wait until it is fully
                // loaded before starting the unload prcess

                // xxx -- implement DeferredUnloadCb

                //tablet->deferUntilState(
                //    new DeferredUnloadCb(this, tabletName),
                //    TABLET_ACTIVE);
            }
            else if(tablet->getState() == TABLET_ACTIVE)
            {
                // Tablet exists and is active: start unload process
                warp::Runnable * callbacks =
                    tablet->setState(TABLET_UNLOAD_COMPACTING);

                if(callbacks)
                    bits.workerPool->submit(callbacks);

                // xxx -- start final serialization
            }
            else
            {
                // xxx -- tablet should be unloading already.  if it's
                // in an error state, that's not so good.
            }
        }

        serverLock.unlock();
        mcb->done();
    }
    catch(std::exception const & ex) {
        mcb->error(ex);
    }
    catch(...) {
        mcb->error(std::runtime_error("unload: unknown exception"));
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
        table->addMemoryFragment(commit.cells, commit.txn);

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
    warp::Callback * cb,
    std::string const & logDir,
    std::string const & tableName,
    warp::Interval<std::string> const & rows)
{
    try {
        workers->fragmentWorkers.submit(
            new LogReplayer(
                cb,
                this,
                logDir,
                tableName,
                rows));
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

void TabletServer::addPendingSerialization(int64_t pendingTxn)
{
    pendingTxns.insert(pendingTxn);
}

void TabletServer::onCompactionStable(
    std::vector<FragmentCPtr> const & oldFragments)
{
    for(std::vector<FragmentCPtr>::const_iterator i = oldFragments.begin();
        i != oldFragments.end(); ++i)
    {
        log("Compaction stable, release %s", (*i)->getFilename());
    }
}

void TabletServer::onSerializationStable(int pendingTxn)
{
    TabletServerLock serverLock(this);

    txn_set::iterator i = pendingTxns.find(pendingTxn);
    assert(i != pendingTxns.end());
    pendingTxns.erase(i);

    // Find the min stable transaction
    bool haveMin = false;
    int64_t minTxn = 0;
    if(pendingTxns.empty())
    {
        for(table_map::const_iterator i = tableMap.begin();
            i != tableMap.end(); ++i)
        {
            Table * table = i->second;
            TableLock tableLock(table);

            int64_t txn = table->getEarliestMemCommit() - 1;
            if(!haveMin || txn < minTxn)
            {
                haveMin = true;
                minTxn = txn;
            }
        }
    }
    else
    {
        haveMin = true;
        minTxn = *pendingTxns.begin();
    }

    warp::Runnable * callbacks = 0;

    if(minTxn > txnCounter.getLastStable())
        callbacks = txnCounter.setLastStable(minTxn);

    if(callbacks)
        bits.workerPool->submit(callbacks);
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

    boost::scoped_ptr<LogWriter> logWriter;
    warp::WallTimer logTimer;

    Commit commit;
    while(logQueue.pop(commit))
    {
        // Start a new log if necessary
        if(!logWriter && bits.logFactory)
        {
            log("Starting new log file");
            logWriter.reset(bits.logFactory->start().release());
            logTimer.reset();
        }

        size_t commitSz = 0;
        int64_t maxTxn;

        // We got a commit to log.  Keep logging until the next queue
        // fetch would block or we've written a significant amount of
        // data.
        do {
            commitSz += commit.cells->getDataSize();
            maxTxn = commit.txn;

            // Log the commit to disk
            if(logWriter)
                logWriter->writeCells(
                    commit.tableName, commit.cells->getPacked());

            // Release the cells
            commit.cells.reset();

        } while( commitSz < GROUP_COMMIT_LIMIT &&
                 logQueue.pop(commit, false) );

        // Sync log to disk.  After this, commits up to maxTxn are
        // durable.
        if(logWriter)
            logWriter->sync();

        TabletServerLock serverLock(this);

        // Update last durable txn
        warp::Runnable * durableCallbacks = txnCounter.setLastDurable(maxTxn);

        serverLock.unlock();

        // Schedule deferred callbacks
        if(durableCallbacks)
            bits.workerPool->submit(durableCallbacks);

        // Roll log if it's big enough
        if(logWriter && logWriter->getDiskSize() >= LOG_SIZE_LIMIT)
        {
            size_t logSz = logWriter->getDiskSize();
            std::string logFn = logWriter->finish();
            double dt = logTimer.getElapsed();
            logWriter.reset();

            log("Finished log file: %s (%sB, %.3f sec, %sB/s)",
                logFn, warp::sizeString(logSz), dt,
                warp::sizeString(size_t(logSz / dt)));

            serverLock.lock();

            /// We can delete the log file after the last commit in
            /// the file is stable (serialized + all configs synced)
            
            if(txnCounter.getLastStable() >= maxTxn)
            {
                // We can delete now
                deleteLogFile(logFn);
            }
            else
            {
                // Delete later
                txnCounter.deferUntilStable(
                    new DeleteLogFileCb(logFn), maxTxn);
            }

            serverLock.unlock();
        }
    }
}
