//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-23
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

#ifndef KDI_SYNCHRONIZED_TABLE_H
#define KDI_SYNCHRONIZED_TABLE_H

#include <kdi/table.h>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>

namespace kdi {

    /// Wrapper class to make a Table thread-safe.
    class SynchronizedTable;

    /// Pointer to a SynchronizedTable.
    typedef boost::shared_ptr<SynchronizedTable> SyncTablePtr;

    /// Pointer to a const SynchronizedTable.
    typedef boost::shared_ptr<SynchronizedTable const> SyncTableCPtr;

} // namespace kdi

//----------------------------------------------------------------------------
// SynchronizedTable
//----------------------------------------------------------------------------
class kdi::SynchronizedTable
    : public kdi::Table,
      public boost::enable_shared_from_this<SynchronizedTable>,
      private boost::noncopyable
{
    /// Thread-safe scanner for SynchronizedTable
    class SynchronizedScanner;

    /// Thread-safe interval scanner for SynchronizedTable
    class SynchronizedIntervalScanner;

    /// Buffered table interface to SynchronizedTable
    class BufferedTable;

    /// Buffered scanner for SynchronizedTable
    class BufferedScanner;

private:
    typedef boost::mutex mutex_t;
    typedef mutex_t::scoped_lock lock_t;

    TablePtr table;
    mutable mutex_t mutex;

private:
    explicit SynchronizedTable(TablePtr const & table);

public:
    /// Create a SynchronizedTable wrapping the given table pointer.
    /// The resulting table may be used safely in a multi-threaded
    /// environment.
    static SyncTablePtr make(TablePtr const & table);

    virtual void set(strref_t row, strref_t column, int64_t timestamp,
                     strref_t value);

    virtual void erase(strref_t row, strref_t column, int64_t timestamp);

    virtual CellStreamPtr scan() const;
    virtual void sync();
    virtual RowIntervalStreamPtr scanIntervals() const;

    /// Create a buffered interface to the SynchronizedTable table,
    /// using default buffer sizes.  Buffering is useful for reducing
    /// locking overhead.  While buffered tables are thread-safe for
    /// scans, they are NOT thread-safe for mutations.  Each mutating
    /// thread must have its own buffered table instance.  That is,
    /// each buffered table instance can have at most one writer
    /// thread and any number of reader threads.  The buffers
    /// coordinate with the SynchronizedTable table in a thread-safe
    /// way, so multiple threads may work with buffers from the same
    /// SynchronizedTable at the same time.
    TablePtr makeBuffer();

    /// Create a buffered interface to the SynchronizedTable table,
    /// using default buffer sizes.  Buffering is useful for reducing
    /// locking overhead.  While buffered tables are thread-safe for
    /// scans, they are NOT thread-safe for mutations.  Each mutating
    /// thread must have its own buffered table instance.  That is,
    /// each buffered table instance can have at most one writer
    /// thread and any number of reader threads.  The buffers
    /// coordinate with the SynchronizedTable table in a thread-safe
    /// way, so multiple threads may work with buffers from the same
    /// SynchronizedTable at the same time.
    /// @param [in] mutationBufferSize Number of mutation operations
    /// to buffer.
    /// @param [in] scanBufferSize Number of Cells to buffer.
    TablePtr makeBuffer(size_t mutationBufferSize, size_t scanBufferSize);
};

#endif // KDI_SYNCHRONIZED_TABLE_H
