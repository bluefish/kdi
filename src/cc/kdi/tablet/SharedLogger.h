//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-15
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

    typedef boost::mutex mutex_t;
    typedef mutex_t::scoped_lock lock_t;

    ConfigManagerPtr configMgr;
    FileTrackerPtr tracker;

    CommitBufferPtr commitBuffer;
    LogWriterPtr logWriter;
    TableGroupPtr tableGroup;
    boost::thread_group threads;

    warp::SyncQueue<CommitBufferCPtr> commitQueue;
    warp::SyncQueue<TableGroupCPtr> serializeQueue;
    mutex_t publicMutex;

public:
    SharedLogger(ConfigManagerPtr const & configMgr,
                 FileTrackerPtr const & tracker);
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
    void flush(lock_t & lock);

    /// Thread loop for committing mutation buffers to disk log.
    void commitLoop();

    /// Thread loop for serializing memory tables to disk tables.
    void serializeLoop();
};

#endif // KDI_TABLET_SHAREDLOGGER_H
