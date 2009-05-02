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
#include <warp/util.h>
#include <kdi/strref.h>
#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <boost/function.hpp>
#include <boost/thread.hpp>
#include <string>
#include <vector>
#include <exception>
#include <tr1/unordered_map>

namespace kdi {
namespace server {

    class TabletServer;

    // Forward declarations
    class Table;
    class Fragment;
    class ConfigReader;
    class ConfigWriter;
    class LogPlayer;
    class FragmentLoader;
    class FragmentMaker;
    class TableSchema;
    class TabletConfig;
    class FragmentWriterFactory;
    class LogWriterFactory;
    typedef boost::shared_ptr<Fragment const> FragmentCPtr;

    class CellBuffer;
    typedef boost::shared_ptr<CellBuffer const> CellBufferCPtr;

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

private:
    mutable boost::mutex serverMutex;

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

    class LoadCb
    {
    public:
        virtual void done() = 0;
        virtual void error(std::exception const & err) = 0;
    protected:
        ~LoadCb() {}
    };

    class UnloadCb
    {
    public:
        virtual void done() = 0;
        virtual void error(std::exception const & err) = 0;
    protected:
        ~UnloadCb() {}
    };

    struct Bits
    {
        LogWriterFactory      * logFactory;
        FragmentWriterFactory * fragmentFactory; 
        ConfigWriter          * configWriter;

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
    /// Find the tabled Table.  Returns null if the table is not
    /// loaded.
    Table * findTable(strref_t tableName) const;

    /// Find a Table which is ready to be serialized
    Table * getSerializableTable() const;

    /// Find a Table which is ready to be compacted
    Table * getCompactableTable() const;

private:
    Table * getTable(strref_t tableName) const;

    void logLoop();

    boost::condition serializeCond;
    bool serializeQuit;
    void serializeLoop();

    boost::condition compactCond;
    bool compactQuit;
    void compactLoop();

    void applySchemas(std::vector<TableSchema> const & schemas);
    void loadTablets(std::vector<TabletConfig> const & configs);

private:
    struct Commit
    {
        std::string tableName;
        int64_t txn;
        CellBufferCPtr cells;
        
        bool operator<(Commit const & o) const
        {
            return warp::order(
                tableName, o.tableName,
                txn, o.txn);
        }

        struct TableNeq
        {
            std::string const & name;
            TableNeq(std::string const & name) : name(name) {}

            bool operator()(Commit const & c) const
            {
                return c.tableName != name;
            }
        };
    };

    class ConfigsLoadedCb;
    class SchemasLoadedCb;

    typedef std::tr1::unordered_map<std::string, Table *> table_map;

private:
    Bits const bits;

    TransactionCounter txnCounter;
    table_map tableMap;

    warp::SyncQueue<Commit> logQueue;
    size_t logPendingSz;

    boost::thread_group threads;
};

#endif // KDI_SERVER_TABLETSERVER_H
