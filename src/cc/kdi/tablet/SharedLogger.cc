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

#include <kdi/tablet/SharedLogger.h>
#include <kdi/tablet/LogWriter.h>
#include <kdi/tablet/Tablet.h>
#include <kdi/tablet/ConfigManager.h>
#include <kdi/tablet/FileTracker.h>
#include <kdi/tablet/LogFragment.h>
#include <kdi/synchronized_table.h>
#include <kdi/scan_predicate.h>
#include <warp/fs.h>
#include <warp/file.h>
#include <warp/uri.h>
#include <warp/log.h>
#include <ex/exception.h>
#include <boost/bind.hpp>
#include <boost/format.hpp>
#include <vector>
#include <string>
#include <sstream>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace ex;
using namespace std;
using boost::format;

#include <kdi/local/disk_table_writer.h>
using kdi::local::DiskTableWriter;

namespace {

    // Max size for the commit buffer before logging to disk
    size_t const COMMIT_BUFFER_SZ = 32 << 10;
    
    // Max size for the table group before serializing
    size_t const SERIALIZE_THRESHOLD_SZ = 128 << 20;

    // Estimate of storage overhead of a cell
    size_t const CELL_OVERHEAD = 6 * sizeof(void *);
    
    /// Get the memory size of a cell
    size_t cellSize(Cell const & x)
    {
        return CELL_OVERHEAD + x.getRow().size() +
            x.getColumn().size() + x.getValue().size();
    }
}

//----------------------------------------------------------------------------
// Concurrency notes
//----------------------------------------------------------------------------

/* There are the following thread groups:
 *   API:        synchronized calls to SharedLogger public API
 *   Commit:     runs SharedLogger::commitLoop()
 *   Serialize:  runs SharedLogger::serializeLoop()
 * 
 *   It is assumed that no two API calls can happen concurrently.
 *   Clients should hold a Synchronized handle to the SharedLogger and
 *   access it through LockedPtrs.
 *
 *   API owns the commitBuffer member.  Once it is full, it is passed
 *   into the commitQueue and a new commitBuffer is created.
 *
 *   Commit owns the tableGroup and logWriter member.  When it is
 *   full, it is passed into the serializeQueue and a new tableGroup
 *   is created.  The TableGroup contains mutable MemTables.  The
 *   Commit thread is the only writer for these tables.  They may be
 *   concurrently read by API and Serialize, although by the time
 *   Serialize reads the table it is no longer mutable.
 *
 *   Since the MemTable requires concurrent access it will be wrapped
 *   in a SynchronizedTable in the TableGroup, making it thread-safe.
 *   The synchronized table is the handle given back to the Tablet.
 *   This is handy since the Tablet initiates scans from this handle
 *   directly.  Since the table is synchronized, the Tablet doesn't
 *   need to do extra coordination.
 *
 *   If the MemTable is recovered from a log file, it will not be
 *   mutable and needn't be synchronized.
 */


//----------------------------------------------------------------------------
// SharedLogger::TableGroup
//----------------------------------------------------------------------------
class SharedLogger::TableGroup
{
    typedef std::map<TabletPtr, LogFragmentPtr> map_t;
    map_t fragmentMap;
    size_t groupSize;
    std::string logUri;

public:
    TableGroup() :
        groupSize(0) {}

    void setLogUri(std::string const & logUri)
    {
        if(logUri.empty())
            raise<ValueError>("empty logUri");

        if(!this->logUri.empty())
            raise<RuntimeError>("already set logUri");

        this->logUri = logUri;
    }

    bool full() const
    {
        return groupSize >= SERIALIZE_THRESHOLD_SZ;
    }

    /// Add a sequence of cells to the mutable table associated with
    /// the given Tablet.  If the Tablet doesn't already have a
    /// mutable table in the group, a new one will be created.  The
    /// log URI is used to derive the table URI when it is added to
    /// the Tablet.
    void applyMutations(TabletPtr const & tablet,
                        std::vector<Cell> const & cells)
    {
        if(logUri.empty())
            raise<RuntimeError>("applyMutations() before setLogUri()");
        
        // Get the fragment for this Tablet
        LogFragmentPtr & frag = fragmentMap[tablet];
        if(!frag)
        {
            // Derive table URI from log URI
            string uri = uriSetParameter(
                logUri, "table",
                uriEncode(tablet->getTableName(), true));

            // Create a new log fragment
            frag.reset(new LogFragment(uri));

            // Notify the Tablet that it has a new fragment
            tablet->addFragment(frag);
        }

        // Add all the cells to the writable table
        TablePtr const & tbl = frag->getWritableTable();
        for(std::vector<Cell>::const_iterator i = cells.begin();
            i != cells.end(); ++i)
        {
            groupSize += cellSize(*i);
            tbl->insert(*i);
        }

        // Flush the mutations
        tbl->sync();
    }

    void serialize(ConfigManagerPtr const & configMgr, FileTrackerPtr const & tracker) const
    {
        log("Serializing %d fragments", fragmentMap.size());

        for(map_t::const_iterator i = fragmentMap.begin();
            i != fragmentMap.end(); ++i)
        {
            TabletPtr tablet = i->first;
            FragmentPtr frag = i->second;

            // Get an output file
            std::string fn = configMgr->getDataFile(tablet->getTableName());
            
            // Write a DiskTable from the fragment
            DiskTableWriter writer(fn, 64<<10);
            CellStreamPtr cells = frag->scan(ScanPredicate());
            Cell x;
            while(cells->get(x))
                writer.put(x);
            writer.close();     // XXX should sync file

            // Reopen the fragment for reading
            std::string diskUri = uriPushScheme(fn, "disk");
            FragmentPtr newFragment = configMgr->openFragment(diskUri);

            // Notify Tablet of the fragment change
            vector<FragmentPtr> oldFragments;
            oldFragments.push_back(frag);
            tablet->replaceFragments(oldFragments, newFragment);

            // Track the new disk file for automatic deletion
            tracker->track(fn);

            // XXX maybe should release memTable.  if a later
            // serialization fails in this group and we go with the
            // wait-and-retry strategy, we'll wind up reserializing.
            // assuming that replaceTables() does nothing if it can't
            // find the oldTable, that's no problem but a waste of
            // time, but it's still a bit ugly.  it creates more files
            // that need to be cleaned up later.
        }

        // All tables referring to the log have been rewritten.
        // Release the log file.
        tracker->release(uriPopScheme(logUri));
    }
};


//----------------------------------------------------------------------------
// SharedLogger::CommitBuffer
//----------------------------------------------------------------------------
class SharedLogger::CommitBuffer
{
    typedef std::vector<Cell> vec_t;
    typedef std::map<TabletPtr, vec_t> map_t;

    map_t tableMap;
    size_t bufferSize;

public:
    CommitBuffer() :
        bufferSize(0) {}

    bool full() const
    {
        return bufferSize >= COMMIT_BUFFER_SZ;
    }

    bool empty() const
    {
        return tableMap.empty();
    }

    void append(TabletPtr const & tablet, Cell const & cell)
    {
        bufferSize += cellSize(cell);
        tableMap[tablet].push_back(cell);
    }

    void writeLog(LogWriterPtr const & logWriter) const
    {
        // For each Tablet, log the buffered cells for that Tablet in
        // a log entry labeled by the Tablet name
        for(map_t::const_iterator i = tableMap.begin();
            i != tableMap.end(); ++i)
        {
            logWriter->logCells(i->first->getTableName(), i->second);
        }

        // Sync the log file to disk
        logWriter->sync();
    }

    void replayToTables(TableGroupPtr const & dstGroup) const
    {
        for(map_t::const_iterator i = tableMap.begin();
            i != tableMap.end(); ++i)
        {
            dstGroup->applyMutations(i->first, i->second);
        }
    }
};


//----------------------------------------------------------------------------
// ErrorState
//----------------------------------------------------------------------------
class SharedLogger::ErrorState
{
public:
    size_t nFailures;
    ostringstream log;
    mutable boost::mutex mutex;
    
    ErrorState() : nFailures(0) {}
    
    void raise() const
    {
        boost::mutex::scoped_lock sync(mutex);
        
        if(nFailures)
            ex::raise<RuntimeError>("%s", log.str());
        else
            ex::raise<RuntimeError>("unknown error");
    }
};


//----------------------------------------------------------------------------
// SharedLogger
//----------------------------------------------------------------------------
SharedLogger::SharedLogger(ConfigManagerPtr const & configMgr,
                           FileTrackerPtr const & tracker) :
    configMgr(configMgr),
    tracker(tracker),
    commitBuffer(new CommitBuffer),
    tableGroup(new TableGroup),
    errorState(new ErrorState),
    commitQueue(4),
    serializeQueue(2)
{
    threads.create_thread(
        boost::bind(&SharedLogger::commitLoop, this));
    threads.create_thread(
        boost::bind(&SharedLogger::serializeLoop, this));
}

SharedLogger::~SharedLogger()
{
    shutdown();
}

void SharedLogger::set(TabletPtr const & tablet, strref_t row, strref_t column,
                       int64_t timestamp, strref_t value)
{
    insert(tablet, makeCell(row, column, timestamp, value));
}

void SharedLogger::erase(TabletPtr const & tablet, strref_t row, strref_t column,
                         int64_t timestamp)
{
    insert(tablet, makeCellErasure(row, column, timestamp));
}

void SharedLogger::insert(TabletPtr const & tablet, Cell const & cell)
{
    lock_t lock(publicMutex);

    if(!commitBuffer)
        raise<RuntimeError>("received mutation after SharedLogger shutdown");

    commitBuffer->append(tablet, cell);
    if(commitBuffer->full())
        flush(lock);
}

void SharedLogger::sync()
{
    lock_t lock(publicMutex);

    if(!commitBuffer)
        raise<RuntimeError>("received flushCommitBuffer() after SharedLogger shutdown");

    flush(lock);

    lock.unlock();
    if(!commitQueue.waitForCompletion())
    {
        lock.lock();

        // If the wait return false, then something canceled the wait
        // before all the work could be done.  This is likely because
        // the Commit or Serialize thread hit an exception.  It's also
        // possible that the SharedLogger got destroyed.  Throw an
        // exception.
        errorState->raise();
    }
}

void SharedLogger::shutdown()
{
    lock_t lock(publicMutex);

    if(commitBuffer)
    {
        log("SharedLogger shutdown");

        flush(lock);

        commitQueue.cancelWaits();
        serializeQueue.cancelWaits();
        threads.join_all();    
        
        commitBuffer.reset();
        logWriter.reset();
        tableGroup.reset();
    }
}

void SharedLogger::flush(lock_t & lock)
{
    // If there's nothing in the buffer, we have nothing to flush.
    if(commitBuffer->empty())
        return;

    //log("Flushing commit buffer");

    // Push the buffer into the Commit thread.  If this fails, it's
    // because the queue is full and waits have been canceled due to
    // an error.
    if(!commitQueue.push(commitBuffer))
        errorState->raise();

    // Make a new commit buffer.
    commitBuffer.reset(new CommitBuffer);

}

void SharedLogger::commitLoop()
{
    // XXX not too happy with the error handling strategy

    log("Commit thread starting");

    try {
    
        for(CommitBufferCPtr buffer;
            commitQueue.pop(buffer);
            buffer.reset())
        {
            //log("Commit thread got work");

            // Create a new log file if we need it
            if(!logWriter)
            {
                string fn = configMgr->getDataFile("LOGS");
                log("Starting new log: %s", fn);
                logWriter.reset(new LogWriter(fn));
                tableGroup->setLogUri(logWriter->getUri());

                // Track file for automatic deletion
                tracker->track(fn);
            }

            // Commit to log
            buffer->writeLog(logWriter);
        
            // Commit to tables
            buffer->replayToTables(tableGroup);

            // Schedule the mutable tables for serialization if they're
            // too big.
            if(tableGroup->full())
            {
                log("Pushing table group for serialization");

                // Push the group to the Serialize thread.  If this
                // fails, it's because the queue is full and waits
                // have been canceled due to an error.
                if(!serializeQueue.push(tableGroup))
                    errorState->raise();

                // Make a new group
                tableGroup.reset(new TableGroup);

                // Clear the log -- we'll make a new one when we have
                // some data to write
                logWriter.reset();
            }

            // Notify queue that we're done with this work
            commitQueue.taskComplete();
        }
    }
    catch(std::exception const & ex) {
        fail("commit", ex.what());
    }
    catch(...) {
        fail("commit", "unknown exception");
    }

    log("Commit thread exiting");
}

void SharedLogger::serializeLoop()
{
    // XXX what do we do with exception here?  if serialization fails,
    // then what?  failures here are likely due to resource problems
    // (e.g. out of disk space).  they (probably) don't interfere with
    // correct client operation, but we'll start having problems if
    // they go on for too long.  maybe we could log the failure, sleep
    // for a bit and try again.  If we use a fixed queue then
    // eventually clients will start backing up and getting timeout
    // errors.

    log("Serialize thread starting");

    try {
        for(TableGroupCPtr group;
            serializeQueue.pop(group);
            group.reset())
        {
            group->serialize(configMgr, tracker);
        }
    }
    catch(std::exception const & ex) {
        fail("Serialize", ex.what());
    }
    catch(...) {
        fail("Serialize", "unknown exception");
    }

    log("Serialize thread exiting");
}

void SharedLogger::fail(char const * who, char const * what)
{
    log("%s thread error: %s", who, what);

    boost::mutex::scoped_lock sync(errorState->mutex);

    if(++errorState->nFailures > 1)
        errorState->log << endl;
    errorState->log << "SharedLogger " << who << " thread failed: "
                    << what;

    serializeQueue.cancelWaits();
    commitQueue.cancelWaits();
}
