//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/SharedLogger.h $
//
// Created 2008/05/15
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_TABLET_SHAREDLOGGER_H
#define KDI_TABLET_SHAREDLOGGER_H

#include <kdi/tablet/forward.h>
#include <kdi/cell.h>
#include <warp/syncqueue.h>
#include <warp/synchronized.h>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/noncopyable.hpp>
#include <map>

namespace kdi {
namespace tablet {

    class SharedLogger;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// SharedLogger
//----------------------------------------------------------------------------
class kdi::tablet::SharedLogger
    : private boost::noncopyable
{
    class CommitBuffer;
    typedef boost::shared_ptr<CommitBuffer> CommitBufferPtr;
    typedef boost::shared_ptr<CommitBuffer const> CommitBufferCPtr;

    class TableGroup;
    typedef boost::shared_ptr<TableGroup> TableGroupPtr;
    typedef boost::shared_ptr<TableGroup const> TableGroupCPtr;

    class ErrorState;
    typedef boost::shared_ptr<ErrorState> ErrorStatePtr;

    ConfigManagerPtr configMgr;

    CommitBufferPtr commitBuffer;
    LogWriterPtr logWriter;
    TableGroupPtr tableGroup;
    boost::thread_group threads;
    ErrorStatePtr errorState;

    warp::SyncQueue<CommitBufferCPtr> commitQueue;
    warp::SyncQueue<TableGroupCPtr> serializeQueue;

public:
    explicit SharedLogger(ConfigManagerPtr const & configMgr);
    ~SharedLogger();

    /// Set a cell in the given tablet. (main thread)
    void set(TabletPtr const & tablet, strref_t row, strref_t column,
             int64_t timestamp, strref_t value);

    /// Erase a cell in the given tablet.
    void erase(TabletPtr const & tablet, strref_t row, strref_t column,
               int64_t timestamp);

    /// Insert a cell (or cell erasure) in the given tablet.
    void insert(TabletPtr const & tablet, Cell const & cell);

    /// Make sure all outstanding mutations are sync'ed to disk.
    void sync();

    /// Shut down shared logger.
    void shutdown();

private:
    /// Flush buffered mutations to the commit thread.
    void flush();

    /// Thread loop for committing mutation buffers to disk log.
    void commitLoop();

    /// Thread loop for serializing memory tables to disk tables.
    void serializeLoop();

    /// This is called when something breaks that we can't handle.
    void fail(char const * who, char const * what);
};

#endif // KDI_TABLET_SHAREDLOGGER_H
