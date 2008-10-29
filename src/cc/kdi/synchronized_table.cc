//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-24
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

#include <kdi/synchronized_table.h>
#include <ex/exception.h>
#include <boost/thread/condition.hpp>
#include <queue>

using namespace kdi;
using namespace warp;
using namespace ex;
using namespace std;

//----------------------------------------------------------------------------
// SynchronizedTable::SynchronizedScanner
//----------------------------------------------------------------------------
class SynchronizedTable::SynchronizedScanner
    : public CellStream
{
    typedef SynchronizedTable::lock_t lock_t;

    SyncTableCPtr table;
    CellStreamPtr scan;

public:
    SynchronizedScanner(SyncTableCPtr const & table,
                        CellStreamPtr const & scan) :
        table(table), scan(scan)
    {
        EX_CHECK_NULL(table);
        EX_CHECK_NULL(scan);
    }

    virtual bool get(Cell & x)
    {
        // One-at-a-time scanner.  Grab the table mutex for each
        // access to the scanner.
        lock_t l(table->mutex);
        return scan->get(x);
    }
};


//----------------------------------------------------------------------------
// SynchronizedTable::SynchronizedIntervalScanner
//----------------------------------------------------------------------------
class SynchronizedTable::SynchronizedIntervalScanner
    : public RowIntervalStream
{
    typedef SynchronizedTable::lock_t lock_t;

    SyncTableCPtr table;
    RowIntervalStreamPtr scan;

public:
    SynchronizedIntervalScanner(SyncTableCPtr const & table,
                                RowIntervalStreamPtr const & scan) :
        table(table), scan(scan)
    {
        EX_CHECK_NULL(table);
        EX_CHECK_NULL(scan);
    }

    virtual bool get(RowInterval & x)
    {
        // One-at-a-time scanner.  Grab the table mutex for each
        // access to the scanner.
        lock_t l(table->mutex);
        return scan->get(x);
    }
};


//----------------------------------------------------------------------------
// SynchronizedTable::BufferedScanner
//----------------------------------------------------------------------------
class SynchronizedTable::BufferedScanner
    : public CellStream
{
    typedef SynchronizedTable::lock_t lock_t;

    SyncTableCPtr table;
    CellStreamPtr scan;
    queue<Cell> buffer;
    size_t bufferSize;

    /// Scan ahead until the buffer is full.  Return true iff there is
    /// something in the buffer after filling.
    bool fillBuffer()
    {
        // Grab the table mutex while we scan
        lock_t l(table->mutex);

        // Buffer cells until the buffer is full or we exhaust the
        // scanner
        Cell x;
        while(buffer.size() < bufferSize && scan->get(x))
            buffer.push(x);

        // Return true if we have something buffered
        return !buffer.empty();
    }

public:
    BufferedScanner(SyncTableCPtr const & table,
                    CellStreamPtr const & scan,
                    size_t bufferSize) :
        table(table), scan(scan), bufferSize(bufferSize)
    {
        EX_CHECK_NULL(table);
        EX_CHECK_NULL(scan);

        if(!bufferSize)
            raise<ValueError>("bufferSize must be positive");
    }

    bool get(Cell & x)
    {
        // Make sure there's something in the buffer
        if(buffer.empty() && !fillBuffer())
            return false;

        // Return the next item out of the buffer
        x = buffer.front();
        buffer.pop();
        return true;
    }
};


//----------------------------------------------------------------------------
// SynchronizedTable::BufferedTable
//----------------------------------------------------------------------------
class SynchronizedTable::BufferedTable
    : public Table,
      private boost::noncopyable
{
    typedef SynchronizedTable::lock_t lock_t;

    SyncTablePtr table;
    queue<Cell> buffer;
    size_t mutationBufferSize;
    size_t scanBufferSize;

    /// Lock table and push all mutations
    void flush()
    {
        // If there's nothing buffered, we don't need to lock
        if(buffer.empty())
            return;

        // Grab the table mutex
        lock_t l(table->mutex);

        // Push all mutations directly to underlying table
        TablePtr const & t = table->table;
        while(!buffer.empty())
        {
            Cell const & x = buffer.front();
            if(x.isErasure())
            {
                // Erase operation
                t->erase(
                    x.getRow(),
                    x.getColumn(),
                    x.getTimestamp()
                    );
            }
            else
            {
                // Set operation
                t->set(
                    x.getRow(),
                    x.getColumn(),
                    x.getTimestamp(),
                    x.getValue()
                    );
            }
            buffer.pop();
        }
    }

public:
    BufferedTable(SyncTablePtr const & table,
                  size_t mutationBufferSize,
                  size_t scanBufferSize) :
        table(table),
        mutationBufferSize(mutationBufferSize),
        scanBufferSize(scanBufferSize)
    {
        EX_CHECK_NULL(table);
    }

    ~BufferedTable()
    {
        // Remember to flush the buffer before losing the table
        flush();
    }

    void set(strref_t row, strref_t column, int64_t timestamp,
             strref_t value)
    {
        // Buffer operation as a Cell
        buffer.push(makeCell(row, column, timestamp, value));

        // Flush the buffer if it's full
        if(buffer.size() >= mutationBufferSize)
            flush();
    }

    void erase(strref_t row, strref_t column, int64_t timestamp)
    {
        // Buffer operation as a Cell
        buffer.push(makeCellErasure(row, column, timestamp));

        // Flush the buffer if it's full
        if(buffer.size() >= mutationBufferSize)
            flush();
    }
    
    CellStreamPtr scan() const
    {
        // If the scan buffer size is 1 or less, then we're really not
        // doing any useful buffering.  Just return the one-at-a-time
        // synchronized scanner.
        if(scanBufferSize <= 1)
            return table->scan();

        // Grab table mutex so we can create a scan from the
        // underlying table
        lock_t l(table->mutex);

        // Create and return a buffered scanner
        CellStreamPtr p(
            new BufferedScanner(
                table,
                table->table->scan(),
                scanBufferSize
                )
            );
        return p;
    }

    void sync()
    {
        // Flush current buffer before syncing
        flush();

        // Sync table
        table->sync();
    }

    RowIntervalStreamPtr scanIntervals() const
    {
        return table->scanIntervals();
    }
};


//----------------------------------------------------------------------------
// SynchronizedScanner
//----------------------------------------------------------------------------
SynchronizedTable::SynchronizedTable(TablePtr const & table) :
    table(table)
{
    EX_CHECK_NULL(table);
}

SyncTablePtr SynchronizedTable::make(TablePtr const & table)
{
    SyncTablePtr p(
        new SynchronizedTable(table)
        );
    return p;
}

void SynchronizedTable::set(strref_t row, strref_t column, int64_t timestamp,
                            strref_t value)
{
    // One-at-a-time mutation.  Grab table mutex and forward operation
    // to underlying table.
    lock_t l(mutex);
    table->set(row, column, timestamp, value);
}

void SynchronizedTable::erase(strref_t row, strref_t column, int64_t timestamp)
{
    // One-at-a-time mutation.  Grab table mutex and forward operation
    // to underlying table.
    lock_t l(mutex);
    table->erase(row, column, timestamp);
}

CellStreamPtr SynchronizedTable::scan() const
{
    // Grab table mutex so we can create a scan from the underlying
    // table.
    lock_t l(mutex);

    // Create and return a one-at-a-time synchronized scanner.
    CellStreamPtr p(
        new SynchronizedScanner(
            shared_from_this(),
            table->scan()
            )
        );
    return p;
}

void SynchronizedTable::sync()
{
    // Grab mutex before syncing underlying table.
    lock_t l(mutex);

    // Sync table.  Note that mutations from all threads are
    // synchronized here, not just the calling thread.
    table->sync();
}

RowIntervalStreamPtr SynchronizedTable::scanIntervals() const
{
    // Grab table mutex so we can create a scan from the underlying
    // table.
    lock_t l(mutex);

    // Create and return a one-at-a-time synchronized scanner.
    RowIntervalStreamPtr p(
        new SynchronizedIntervalScanner(
            shared_from_this(),
            table->scanIntervals()
            )
        );
    return p;
}

TablePtr SynchronizedTable::makeBuffer()
{
    // Return a buffer with some default buffer sizes.  This number is
    // pretty arbitrary.
    return makeBuffer(32, 32);
}

TablePtr SynchronizedTable::makeBuffer(size_t mutationBufferSize,
                                       size_t scanBufferSize)
{
    TablePtr p(
        new BufferedTable(
            shared_from_this(),
            mutationBufferSize,
            scanBufferSize
            )
        );
    return p;
}

//----------------------------------------------------------------------------
// Registration
//----------------------------------------------------------------------------
#include <kdi/table_factory.h>
#include <warp/init.h>
#include <warp/uri.h>
#include <warp/strutil.h>
#include <boost/weak_ptr.hpp>
#include <map>

using namespace warp;

namespace {

    /// Weak pointer to a SynchronizedTable.
    typedef boost::weak_ptr<SynchronizedTable> SyncTableWeakPtr;

    TablePtr openSyncTable(string const & uri)
    {
        typedef std::map<std::string, SyncTableWeakPtr> map_t;
        typedef boost::mutex mutex_t;
        typedef mutex_t::scoped_lock lock_t;
        typedef boost::condition condition_t;

        // Global variables
        static map_t cache;
        static mutex_t cacheMutex;
        static bool creatingTable = false;
        static condition_t creationDone;

        // Get buffer default sizes
        size_t writeCount = 1;
        size_t readCount = 1;
        if(uriTopScheme(uri) == "syncbuffer")
        {
            writeCount = 64;
            readCount = 64;
        }

        // Parse our parameters
        parseInt(writeCount, wrap(uriGetParameter(uri, "syncWriteCount")));
        parseInt(readCount, wrap(uriGetParameter(uri, "syncReadCount")));

        // Get the cache key: the URI with the sync scheme parameters
        // stripped out
        string baseUri = uriPopScheme(
            uriEraseParameter(
                uriEraseParameter(
                    uri,
                    "syncWriteCount"),
                "syncReadCount"));
        string uriKey = uriNormalize(baseUri);

        // Get the SynchronizedTable for the key -- we loop until
        // syncTable is not null or we throw from a table error.  The
        // only time we will loop is if the table is not in the cache
        // and another thread is currently creating a new
        // SynchronizedTable through this function.
        SyncTablePtr syncTable;
        for(;;)
        {
            // Access to the cache must be synchronized
            lock_t cacheLock(cacheMutex);

            // Return the key if we can get it from the cache
            map_t::const_iterator i = cache.find(uriKey);
            if(i != cache.end())
            {
                // Found an entry -- try to lock the weak pointer
                syncTable = i->second.lock();
                if(syncTable)
                {
                    // Got it - done
                    break;
                }
            }

            // Not in the cache -- we need to make the table.

            // Only one thread is allowed to create new tables at a
            // time.  If we're not it, loop and try again later.
            if(creatingTable)
            {
                creationDone.wait(cacheLock);
                continue;
            }

            // We're it -- set the flag
            creatingTable = true;

            // Unlock before trying to make a table 
            cacheLock.unlock();

            try {
                // Make a new table
                syncTable = SynchronizedTable::make(
                    Table::open(baseUri)
                    );
            }
            catch(...) {
                // Clear the flag before we bail.  We don't need to
                // lock to clear the flag, only setting it is
                // critical.
                creatingTable = false;
                creationDone.notify_all();
                throw;
            }

            // Relock, clear the flag, and insert into the cache
            cacheLock.lock();
            creatingTable = false;
            creationDone.notify_all();
            cache[uriKey] = syncTable;

            // We have a table, so break
            break;
        }

        // We have the sync table.  If the buffers parameters are
        // greater than 1, we should return a buffered window to the
        // table.  Otherwise, just return the sync table directly.
        if(writeCount > 1 || readCount > 1)
        {
            return syncTable->makeBuffer(writeCount, readCount);
        }
        else
        {
            return syncTable;
        }
    }

}

WARP_DEFINE_INIT(kdi_synchronized_table)
{
    TableFactory::get().registerTable("sync", &openSyncTable);
    TableFactory::get().registerTable("syncbuffer", &openSyncTable);
}
