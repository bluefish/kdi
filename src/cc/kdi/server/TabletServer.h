//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/server/TabletServer.h $
//
// Created 2009/02/25
//
// Copyright 2009 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_SERVER_TABLETSERVER_H
#define KDI_SERVER_TABLETSERVER_H

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
    FragmentCPtr decodeCells(strref_t packedCells) const;

    Table * getTable(strref_t tableName) const;

    int64_t getLastCommitTxn() const;
    int64_t assignCommitTxn();

    int64_t getLastDurableTxn() const;

    size_t assignScannerId();

    void queueCommit(FragmentCPtr const & fragment, int64_t commitTxn);

    void deferUntilDurable(ApplyCb * cb, int64_t commitTxn);
    void deferUntilDurable(SyncCb * cb, int64_t waitForTxn);

private:
    std::tr1::unordered_map<std::string, Table *> tableMap;
};

#endif // KDI_SERVER_TABLETSERVER_H
