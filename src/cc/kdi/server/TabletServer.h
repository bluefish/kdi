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

#include <boost/noncopyable.hpp>

namespace kdi {
namespace server {

    class TabletServer;

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
                     strref_t table,
                     strref_t packedCells,
                     int64_t commitMaxTxn,
                     bool waitForSync);

    void sync_async(SyncCb * cb, int64_t waitForTxn);


private: // mess in progress
    CellBufferPtr decodeCells(strref_t packedCells) const;

    Table * getTable(string const & tableName) const;

    int64_t getLastCommitTxn() const;
    int64_t assignCommitTxn();

    size_t assignScannerId();

    void queueCommit(CellBufferPtr const & cells, int64_t commitTxn);

    void deferUntilDurable(ApplyCb * cb, int64_t commitTxn);
    void deferUntilDurable(SyncCb * cb, int64_t waitForTxn);

private:
    boost::mutex serverMutex;
};

#endif // KDI_SERVER_TABLETSERVER_H
