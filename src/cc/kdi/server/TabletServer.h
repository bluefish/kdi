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

#ifndef KDI_SERVER_TABLETSERVER_H
#define KDI_SERVER_TABLETSERVER_H

#include <kdi/server/TransactionCounter.h>
#include <warp/syncqueue.h>
#include <warp/WorkerPool.h>
#include <warp/Callback.h>
#include <warp/util.h>
#include <kdi/strref.h>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread.hpp>
#include <string>
#include <vector>
#include <exception>
#include <tr1/unordered_map>

namespace kdi {
namespace server {

    class TabletServer;
    class TabletServerLock;

    // Forward declarations
    class Table;
    class Fragment;
    class SchemaReader;
    class ConfigReader;
    class ConfigWriter;
    class LogPlayer;
    class FragmentLoader;
    class FragmentMaker;
    class TableSchema;
    class TabletConfig;
    class FragmentWriterFactory;
    class LogWriterFactory;
    class CellBuffer;

    typedef boost::shared_ptr<Fragment const> FragmentCPtr;
    typedef boost::shared_ptr<CellBuffer const> CellBufferCPtr;
    typedef boost::shared_ptr<TableSchema const> TableSchemaCPtr;
    typedef boost::shared_ptr<TabletConfig const> TabletConfigCPtr;

    typedef boost::shared_ptr<std::vector<TabletConfig> const> TabletConfigVecCPtr;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// TabletServer
//----------------------------------------------------------------------------
class kdi::server::TabletServer
    : private boost::noncopyable
{
public:
    enum { MAX_TXN = 9223372036854775807 };

public:
    class ApplyCb
    {
    public:
        virtual void done(int64_t commitTxn) = 0;
        virtual void error(std::exception const & err) = 0;
    protected:
        ~ApplyCb() {}
    };

    class SyncCb
    {
    public:
        virtual void done(int64_t syncTxn) = 0;
        virtual void error(std::exception const & err) = 0;
    protected:
        ~SyncCb() {}
    };

    typedef warp::Callback LoadCb;
    typedef warp::Callback UnloadCb;

    struct Bits
    {
        LogWriterFactory      * logFactory;
        FragmentWriterFactory * fragmentFactory; 
        ConfigWriter          * configWriter;

        SchemaReader          * schemaReader;
        ConfigReader          * configReader;
        LogPlayer             * logPlayer;
        FragmentLoader        * fragmentLoader;

        warp::WorkerPool      * workerPool;
        std::string             serverLogDir;
        std::string             serverLocation;

        Bits() :
            logFactory(0),
            fragmentFactory(0),
            configWriter(0),

            schemaReader(0),
            configReader(0),
            logPlayer(0),
            fragmentLoader(0),

            workerPool(0)
        {}
    };

    typedef std::vector<std::string> string_vec;

public:
    explicit TabletServer(Bits const & bits);
    ~TabletServer();

    /// Load some tablets.  Tablet names should be given in sorted
    /// order for best performance.
    void load_async(LoadCb * cb, string_vec const & tablets);

    /// Unload some tablets.
    void unload_async(UnloadCb * cb, string_vec const & tablets);

    /// Apply block of cells to the named table.  The cells will only
    /// be applied if the server can guarantee that none of the rows
    /// in packedCells have been modified more recently than
    /// commitMaxTxn.  If the mutation should be applied
    /// unconditionally, use MAX_TXN.  If waitForSync is true, wait
    /// until the commit has been made durable before issuing the
    /// callback.
    void apply_async(ApplyCb * cb,
                     strref_t tableName,
                     strref_t packedCells,
                     int64_t commitMaxTxn,
                     bool waitForSync);

    /// Wait until the given commit transaction has been made durable.
    /// If the given transaction is greater than the last assigned
    /// commit number, wait for the last assigned commit instead.
    void sync_async(SyncCb * cb, int64_t waitForTxn);

public:
    std::string const & getLogDir() const { return bits.serverLogDir; }
    std::string const & getLocation() const { return bits.serverLocation; }

public:
    class LoadSchemaCb
    {
    public:
        virtual void done(TableSchemaCPtr const & schema) = 0;
        virtual void error(std::exception const & err) = 0;
    protected:
        ~LoadSchemaCb() {}
    };

    class LoadConfigCb
    {
    public:
        virtual void done(TabletConfigCPtr const & config) = 0;
        virtual void error(std::exception const & err) = 0;
    protected:
        ~LoadConfigCb() {}
    };

    class LoadFragmentsCb
    {
    public:
        virtual void done(std::vector<FragmentCPtr> const & fragments) = 0;
        virtual void error(std::exception const & err) = 0;
    protected:
        ~LoadFragmentsCb() {}
    };


private:
    /// Initiate the multi-step sequence for loading a Tablet.  First,
    /// the Tablet's config is loaded.  If the config specifies a log
    /// directory, the Tablet's logs will be replayed.  Then the
    /// Tablet's config will be saved using this server's location and
    /// log.  Finally, all necessary disk fragments will be loaded.
    /// When all of this is complete, issue the callback.  This
    /// routine assumes the Tablet exists and is in the initial
    /// loading state.
    // xxx -- void loadTablet_async(warp::Callback * cb, std::string const & tabletName);

    /// Load the table schema for a Table.  The callback is given the
    /// loaded TabletSchema object.
    void loadSchema_async(LoadSchemaCb * cb, std::string const & tableName);

    /// Load the config for a Tablet.  The callback is given a shared
    /// handle to the loaded config object.
    void loadConfig_async(LoadConfigCb * cb, std::string const & tabletName);

    /// Replay the logs from the given log dir and apply commits for
    /// the named Tablet.  This routine assumes the named Tablet
    /// exists and is in the TABLET_LOG_REPLAYING state.  The callback
    /// is issued when the log replay has completed.
    void replayLogs_async(warp::Callback * cb, std::string const & tabletName,
                          std::string const & logDir);

    /// Save the given TabletConfig and issue a callback when it is
    /// durable.
    void saveConfig_async(warp::Callback * cb, TabletConfigCPtr const & config);

    /// Load the fragments named in the given Tablet config.  The
    /// callback is given a vector of handles to the loaded fragments.
    /// The loaded fragment list may not correspond 1:1 with the
    /// config fragment list, but it will yield equivalent results
    /// when merged in order.
    void loadFragments_async(LoadFragmentsCb * cb, TabletConfigCPtr const & config);

public:
    /// Find the tabled Table.  Returns null if the table is not
    /// loaded.
    Table * findTable(strref_t tableName) const;

private:
    Table * findTableLocked(strref_t tableName) const;
    Table * getTable(strref_t tableName) const;

    void wakeSerializer();
    void wakeCompactor();

    void logLoop();

private:
    struct Commit
    {
        std::string tableName;
        int64_t txn;
        CellBufferCPtr cells;
    };

    class SchemaLoadedCb;

    class ConfigLoadedCb;
    class LogReplayedCb;
    class ConfigSavedCb;
    class FragmentsLoadedCb;

    class Workers;
    class SWork;
    class SInput;
    class CWork;
    class CInput;

    typedef std::tr1::unordered_map<std::string, Table *> table_map;

private:
    Bits const bits;

    TransactionCounter txnCounter;
    table_map tableMap;

    warp::SyncQueue<Commit> logQueue;
    size_t logPendingSz;

    boost::thread_group threads;
    boost::scoped_ptr<Workers> workers;

private:
    friend class TabletServerLock;
    mutable boost::mutex serverMutex;
};

//----------------------------------------------------------------------------
// TabletServerLock
//----------------------------------------------------------------------------
class kdi::server::TabletServerLock
    : private boost::noncopyable
{
public:
    explicit TabletServerLock(TabletServer const * server) :
        _lock(server->serverMutex) {}

    TabletServerLock(TabletServer const * server, bool doLock) :
        _lock(server->serverMutex, doLock) {}

    void lock() { _lock.lock(); }
    void unlock() { _lock.unlock(); }
    bool locked() const { return _lock.locked(); }

private:
    boost::mutex::scoped_lock _lock;
};


#endif // KDI_SERVER_TABLETSERVER_H
