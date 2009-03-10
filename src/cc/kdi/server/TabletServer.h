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

#include <warp/syncqueue.h>
#include <warp/util.h>
#include <kdi/strref.h>
#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>
#include <string>
#include <exception>
#include <tr1/unordered_map>

namespace kdi {
namespace server {

    class TabletServer;

    // Forward declarations
    class Table;
    class Fragment;
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
    boost::mutex serverMutex;

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

public:
    void apply_async(ApplyCb * cb,
                     strref_t tableName,
                     strref_t packedCells,
                     int64_t commitMaxTxn,
                     bool waitForSync);

    void sync_async(SyncCb * cb, int64_t waitForTxn);


    // mess in progress
public:
    /// Get the tabled Table.  Returns null if the table is not
    /// loaded.
    Table * tryGetTable(strref_t tableName) const;

private:
    Table * getTable(strref_t tableName) const;

    int64_t getLastCommitTxn() const;
    int64_t assignCommitTxn();

    int64_t getLastDurableTxn() const;
    void setLastDurableTxn(int64_t txn);

    size_t assignScannerId();

    void deferUntilDurable(ApplyCb * cb, int64_t commitTxn);
    void deferUntilDurable(SyncCb * cb, int64_t waitForTxn);

    void logLoop();

    void addMemFragment(strref_t tableName, Fragment const * fragment);

    void scheduleSerialization();
    bool serializationPending;

private:
    std::tr1::unordered_map<std::string, Table *> tableMap;

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

    warp::SyncQueue<Commit> logQueue;

    size_t commitBufferSz;
    size_t activeBufferSz;
};

#endif // KDI_SERVER_TABLETSERVER_H
