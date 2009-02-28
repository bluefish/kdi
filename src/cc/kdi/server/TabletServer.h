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

namespace kdi {
namespace server {

    class TabletServer;

    // Forward declarations
    class Table;
    class CellBuffer;
    typedef boost::shared_ptr<CellBuffer> CellBufferPtr;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// TabletServer
//----------------------------------------------------------------------------
class kdi::server::TabletServer
    : private boost::noncopyable
{
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
    Table * tryGetTable(strref_t tableName) const;

private:
    CellBufferPtr decodeCells(strref_t packedCells) const;

    Table * getTable(strref_t tableName) const;

    int64_t getLastCommitTxn() const;
    int64_t assignCommitTxn();

    int64_t getLastDurableTxn() const;

    size_t assignScannerId();

    void queueCommit(CellBufferPtr const & cells, int64_t commitTxn);

    void deferUntilDurable(ApplyCb * cb, int64_t commitTxn);
    void deferUntilDurable(SyncCb * cb, int64_t waitForTxn);

private:
    boost::mutex serverMutex;
};

#endif // KDI_SERVER_TABLETSERVER_H
