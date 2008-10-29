//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-12
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

#include <kdi/local/local_table.h>
#include <kdi/local/disk_table.h>
#include <kdi/local/disk_table_writer.h>
#include <kdi/logged_memory_table.h>
#include <kdi/cell_merge.h>
#include <kdi/cell_filter.h>
#include <kdi/scan_predicate.h>
#include <kdi/synchronized_table.h>
#include <warp/file.h>
#include <warp/syncqueue.h>
#include <warp/fs.h>
#include <warp/config.h>
#include <warp/uri.h>
#include <warp/log.h>
#include <ex/exception.h>

#include <boost/format.hpp>
#include <boost/thread.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <list>
#include <sstream>
#include <math.h>

using namespace kdi;
using namespace kdi::local;
using namespace warp;
using namespace ex;
using namespace std;
using boost::format;

//----------------------------------------------------------------------------
// Constants
//----------------------------------------------------------------------------
namespace {
    size_t const MAX_TABLE_MEMORY = 128 << 20; // 128 mb
    size_t const MAX_WAY_MERGE = 16;
}

//----------------------------------------------------------------------------
// Todo
//----------------------------------------------------------------------------
/*

  - LocalTables start taking up too much memory -- they each have
    independent notions of max resident memory size.  There needs to
    be some kind of shared memory management.

  - Each LocalTable makes its own worker threads, each with its own 10
    MB stack, so just opening a table takes 20 MB.  The threads ought
    to be shared.

  - We'd like some basic system-managed Table GC policies it could be
    implemented in the Table implementation, which has performance
    advantages, or it could be an external daemon that periodically
    sweeps and erases old data from tables using the normal interface.
    that has the advantage that it's general (for shared-access
    tables).

  - A way to clean up garbage files in LocalTable directory seems to
    imply a need for a way to map a Table to a set of files.  no,
    instead scan directory for numbered files.  anything found that
    doesn't match with something in the config file should be deleted.

  - An error propagation mechanism for background tasks that run into
    problems
    - for now, exit with error message.  this is bad because it leaves
      the lock file. whirr.

  - Decide when and what to compact.  We could use some extra
    information to make good decisions.  If we track the proportion of
    erasures, it can help in the decision of when to do a full
    compaction.  Also, we might be able to do something with hashes of
    set cells to see the approximate overlap for overwritten cells.
    OTOH, that's probably tricky and not a very big reward.

*/

//----------------------------------------------------------------------------
// Lockfile
//----------------------------------------------------------------------------
namespace {

    /// Ensures that a directory exists.
    class AutoDirectory : public std::string
    {
    public:
        explicit AutoDirectory(string const & dir) :
            std::string(dir)
        {
            fs::makedirs(dir);
        }
    };

    /// RAII wrapper for a lock file.  If the file does not exist, it
    /// is created and the lock is "acquired".  If the file already
    /// exists, then some other object is holding the file and the
    /// constructor throws an exception.  The lock file is cleaned up
    /// when the object goes out of scope.
    class Lockfile
    {
        string fn;
        bool locked;

    public:
        explicit Lockfile(string const & fn) :
            fn(fn), locked(false)
        {
            // cerr << "Lockfile: " << fn << endl;

            // Exclusive-create file or throw exception
            File::open(fn, O_CREAT|O_WRONLY|O_TRUNC|O_EXCL);

            // We created the file
            locked = true;
        }

        ~Lockfile()
        {
            // cerr << "~Lockfile: " << fn << endl;

            if(locked)
            {
                // Remove file on exit
                fs::remove(fn);
            }
        }
    };
}

//----------------------------------------------------------------------------
// LocalTable::Impl
//----------------------------------------------------------------------------
class LocalTable::Impl
    : public boost::enable_shared_from_this<Impl>
{
    struct TableInfo
    {
        TableCPtr table;
        string name;
        size_t tableSize;
        bool isDiskTable;

        TableInfo(TableCPtr const & table,
                  string const & name,
                  size_t tableSize,
                  bool isDiskTable) :
            table(table),
            name(name),
            tableSize(tableSize),
            isDiskTable(isDiskTable)
        {
        }
    };

    typedef list<TableInfo> table_list_t;
    typedef boost::mutex mutex_t;
    typedef mutex_t::scoped_lock lock_t;

    // directory containing all table files
    AutoDirectory const tableDir;

    // directory-level lock for table
    Lockfile const lockFile;

    // index of next output file
    size_t nextIndex;           // synchronize

    // ordered set of all readable tables (including current writable table)
    table_list_t readableTables; // synchronize

    // current writable table
    MemoryTablePtr memTable;
    TablePtr writableTable;

    // mutex on synchronized members.  this is only used to coordinate
    // between the main thread and the worker threads that do
    // background operations on the table.  the main table interface
    // is not thread-safe.
    mutable mutex_t mutex;

    // worker command queue
    SyncQueue< boost::function<void ()> > serializationQueue;
    SyncQueue< boost::function<void ()> > compactionQueue;

    // worker thread
    boost::scoped_ptr<boost::thread> serializationThread;
    boost::scoped_ptr<boost::thread> compactionThread;

    // has the table been initialized?
    bool initialized;

    // is there a compaction scheduled?
    bool compactionScheduled;   // synchronize
    bool fullCompactionRequested; // synchronize

    // Event to indicate a compaction has finished
    boost::condition compactionFinished;

    void writeConfig()
    {
        // This function is not thread-safe.  It is assumed that the
        // caller has handled locking.

        // Write a new Config reflecting current table coordination
        // state

        // State should include:
        //   - names for opening all the readable tables
        //   - next file index
        //   - number of tables properly serialized

        Config cfg;
        cfg.set("nextIndex", str(format("%d") % nextIndex));
        size_t nDisk = 0;
        size_t nTables = 0;
        for(table_list_t::const_iterator ti = readableTables.begin();
            ti != readableTables.end(); ++ti)
        {
            cfg.set(str(format("table.i%d") % nTables),
                    ti->name);
            ++nTables;
            if(ti->isDiskTable)
                ++nDisk;
        }
        cfg.set("numDiskTables", str(format("%d") % nDisk));

        // Write to temp file, rename to output
        string cfgFn = fs::resolve(tableDir, "state");
        string tmpFn = fs::changeExtension(cfgFn, ".tmp");
        cfg.writeProperties(tmpFn);
        fs::rename(tmpFn, cfgFn, true);

        // cerr << "Wrote config:" << endl << cfg << endl;
    }

    void serializeTable(table_list_t::iterator it)
    {
        // Lock and get index for output file
        lock_t l(mutex);
        size_t idx = nextIndex++;
        writeConfig();
        l.unlock();

        // Open output
        string name = str(format("%d") % idx);
        string fn = fs::resolve(tableDir, name);
        DiskTableWriter writer(fn, 64 << 10);

        // cerr << format("serialize: %s --> %s") % it->name % name << endl;
        log("starting serialization: %s --> %s", it->name, name);

        // Write table
        CellStreamPtr scan = it->table->scan();
        Cell x;
        while(scan->get(x))
            writer.put(x);
        writer.close();

        // Open table for reading
        TablePtr tbl(new DiskTable(fn));
        
        // Lock and swap table in
        TableInfo tmp(tbl, name, fs::filesize(fn), true);
        l.lock();
        swap(*it, tmp);
        writeConfig();
        l.unlock();

        // Remove files associated with old table
        fs::remove(fs::resolve(tableDir, tmp.name));

        // Done
        // cerr << format("serialize done: %s --> %s") % tmp.name % it->name << endl;
        log("serialization done: %s --> %s", tmp.name, it->name);
    }

    void compactTables(table_list_t::iterator first, table_list_t::iterator last,
                       bool filterErasures)
    {
        // The last table is always writable and we should never be
        // compacting it.  Also we depend on the fact that
        // [first,last) is a fixed-size range.  If the last iterator
        // is END, then any new tables inserted in the list while we
        // were compacting would also be included in the range, which
        // is bad when we remove the range from the table list.
        // However, the two runtime operations on the table list are
        // 1) append at the end, and 2) remove some internal nodes in
        // a compaction.  So if our last iterator is not END, no
        // tables can be inserted into it when we're not looking.
        assert(last != readableTables.end());

        // We also assume that [first,last) is a non-empty range
        assert(first != last);

        // Lock and get index for output file
        lock_t l(mutex);
        size_t idx = nextIndex++;
        writeConfig();
        l.unlock();

        // Open output
        string name = str(format("%d") % idx);
        string fn = fs::resolve(tableDir, name);
        DiskTableWriter writer(fn, 64 << 10);

        ostringstream info;

        // Create merge stream -- streams must be added in reverse
        // order since earlier inputs to the merge come first on
        // identical keys
        vector<string> oldFiles;
        CellStreamPtr merge = CellMerge::make(filterErasures);
        for(table_list_t::iterator it = last; it != first; )
        {
            --it;
            info << it->name << ' ';
            merge->pipeFrom(it->table->scan());
            oldFiles.push_back(it->name);
        }

        info << "--> " << name;
        // cerr << "compact: " << info.str() << endl;
        log("starting %scompaction: %s",
            filterErasures ? "major " : "",
            info.str());

        // Write table
        Cell x;
        while(merge->get(x))
            writer.put(x);
        writer.close();

        // Open table for reading
        TablePtr tbl(new DiskTable(fn));
        
        // Lock and replace tables with compacted result. Remove
        // [first, last-1), replace last, and write config.
        l.lock();
        --last;
        readableTables.erase(first, last);
        *last = TableInfo(tbl, name, fs::filesize(fn), true);
        writeConfig();
        l.unlock();

        // Remove files associated with old tables
        for(vector<string>::const_iterator i = oldFiles.begin();
            i != oldFiles.end(); ++i)
        {
            fs::remove(fs::resolve(tableDir, *i));
        }

        // Done
        // cerr << "compact done: " << info.str() << endl;
        log("compaction done: %s", info.str());

        l.lock();
        compactionScheduled = false;
        compactionFinished.notify_all();

        // Schedule a full compaction if a request showed up while we
        // were working
        if(fullCompactionRequested)
        {
            fullCompactionRequested = false;
            scheduleFullCompaction();
        }
        else if(readableTables.size() > 8)
        {
            scheduleCompaction(readableTables.size() - 4);
        }
    }

    void createWritableTable()
    {
        // Assumes that mutex is locked

        // Get name for next writable table
        size_t idx = nextIndex++;
        string name = str(format("%d") % idx);
        string fn = fs::resolve(tableDir, name);

        // Create a new logged memory table
        memTable = LoggedMemoryTable::create(fn, false);

        // Make a buffered synchronized interface on memory table
        writableTable = SynchronizedTable::make(memTable)->makeBuffer();

        // Writable table is also readable, so add it to readable list
        readableTables.push_back(TableInfo(writableTable, name, 0, false));

        // Write config file
        writeConfig();

        // If we have too many tables, schedule a compaction
        if(!compactionScheduled && readableTables.size() > 8)
            scheduleCompaction(readableTables.size() - 4);
    }

    void startNewWritableTable()
    {
        assert(writableTable);

        // Sync and release old writable table
        writableTable->sync();
        writableTable.reset();
        memTable.reset();

        // Grab mutex for access to readableTables and call to
        // createWritableTable()
        lock_t l(mutex);

        // Schedule it for serialization (it is the last readableTable)
        log("scheduling memtable for serialization");
        serializationQueue.push(
            boost::bind(
                &Impl::serializeTable,
                shared_from_this(),
                --readableTables.end()
                )
            );

        // Create a new writable table
        createWritableTable();
    }

    void workerLoop(SyncQueue< boost::function<void ()> > & workerQueue)
    {
        // cerr << "worker thread starting" << endl;

        for(;;)
        {
            // Get next command
            boost::function<void ()> cmd;
            if(!workerQueue.pop(cmd))
            {
                // Got nothing -- queue must have been cancelled
                break;
            }

            // Run command
            try {
                cmd();
            }
            catch(Exception const & ex) {
                cerr << "workerThread: " << ex << endl;
                exit(1);
            }
            catch(std::exception const & ex) {
                cerr << "workerThread error: " << ex.what() << endl;
                exit(1);
            }
            catch(...) {
                cerr << "workerThread error" << endl;
                exit(1);
            }
        }

        // cerr << "worker thread exiting" << endl;
    }

    void loadFromConfig(Config const & cfg)
    {
        // cerr << "Loaded config:" << endl << cfg << endl;

        nextIndex = cfg.getAs<size_t>("nextIndex");
        size_t nDisk = cfg.getAs<size_t>("numDiskTables");
        Config const & tables = cfg.getChild("table");
        for(size_t i = 0; i < tables.numChildren(); ++i)
        {
            bool isDiskTable = i < nDisk;
            string name = tables.getChild(i).get();
            string fn = fs::resolve(tableDir, name);
            
            // cerr << "Loading table: " << name << endl;
            log("loading table: %s", name);

            TablePtr tbl;
            if(isDiskTable)
                tbl.reset(new DiskTable(fn));
            else
                tbl = LoggedMemoryTable::create(fn, false);

            size_t tableSz = fs::filesize(fn);
            {
                lock_t l(mutex);
                readableTables.push_back(TableInfo(tbl, name, tableSz, isDiskTable));
            }

            // Schedule serializations for reloaded memory tables
            if(!isDiskTable)
            {
                log("scheduling old memtable for serialization");
                serializationQueue.push(
                    boost::bind(
                        &Impl::serializeTable,
                        shared_from_this(),
                        --readableTables.end()
                        )
                    );
            }
        }
    }

    static float readCost(table_list_t::const_iterator first,
                          table_list_t::const_iterator last)
    {
        size_t n = 0;
        size_t sz = 0;
        for(table_list_t::const_iterator it = first; it != last; ++it, ++n)
        {
            assert(it->isDiskTable);
            sz += it->tableSize;
        }
        return sz * logf(n + 1);
    }

    void scheduleFullCompaction()
    {
        // Table mutex is assumed to be locked

        log("requested full compaction");

        assert(!compactionScheduled);

        // If we have too many tables, schedule a partial compaction instead
        if(readableTables.size() > MAX_WAY_MERGE)
        {
            scheduleCompaction(readableTables.size() - 4);
            fullCompactionRequested = true;
            return;
        }

        table_list_t::iterator begin = readableTables.begin();
        table_list_t::iterator end = begin;
        size_t nTables = 0;
        while(end != readableTables.end() && end->isDiskTable)
        {
            ++nTables;
            ++end;
        }

        if(nTables > 1)
        {
            log("scheduling full compaction of %d tables", nTables);

            // Got a compaction range, schedule it
            compactionQueue.push(
                boost::bind(
                    &Impl::compactTables,
                    shared_from_this(),
                    begin,
                    end,
                    true
                    )
                );
                
            compactionScheduled = true;
        }
    }

    void scheduleCompaction(size_t minWayMerge)
    {
        // Table mutex is assumed to be locked

        log("requested compaction of at least %d tables", minWayMerge);

        assert(!compactionScheduled);
        assert(minWayMerge > 1);

        compactionScheduled = true;

        // Get the range of disk tables
        table_list_t::iterator begin = readableTables.begin();
        table_list_t::iterator end = begin;
        size_t baseSum = 0;
        size_t baseCount = 0;
        while(end != readableTables.end() && end->isDiskTable)
        {
            baseSum += end->tableSize;
            ++baseCount;
            ++end;
        }

        // Don't bother if there's not enough around to compact
        if(baseCount <= 1)
            return;

        // HACK - KFS has problems when we open many file handles --
        // limit merge width
        if(minWayMerge > MAX_WAY_MERGE)
            minWayMerge = MAX_WAY_MERGE;

        // Get the cost of reading the entire disk-based table without
        // doing a compaction
        float baseCost = baseSum * logf(baseCount + 1);

        // Find the best compaction window
        std::pair<table_list_t::iterator, table_list_t::iterator> bestRange(begin, end);
        float bestScore = 0;
        for(table_list_t::iterator first = begin; first != end; ++first)
        {
            table_list_t::iterator last = first;

            size_t rangeCount = 0;
            size_t rangeSum = 0;
            while(last != end && rangeCount < minWayMerge-1)
            {
                rangeSum += last->tableSize;
                ++rangeCount;
                ++last;
            }
            if(last == end)
                continue;

            while(last != end)
            {
                rangeSum += last->tableSize;
                ++rangeCount;
                ++last;

                float rangeCost = rangeSum * logf(rangeCount + 1);
                float postCost = baseSum * logf(baseCount - rangeCount + 2);

                float score = (baseCost - postCost) / rangeCost;
                
                if(score > bestScore)
                {
                    bestScore = score;
                    bestRange.first = first;
                    bestRange.second = last;
                }

                if(rangeCount == MAX_WAY_MERGE)
                    break;
            }
        }

        // Got a compaction range, schedule it
        log("scheduling compaction of %d tables",
            std::distance(bestRange.first, bestRange.second));

        compactionQueue.push(
            boost::bind(
                &Impl::compactTables,
                shared_from_this(),
                bestRange.first,
                bestRange.second,
                bestRange.first == readableTables.begin()
                )
            );
    }

public:
    Impl(string const & tableDir) :
        tableDir(tableDir),
        lockFile(fs::resolve(tableDir, "lock")),
        nextIndex(0),
        serializationQueue(2),  // block if we have more than 2 serializations outstanding
        initialized(false),
        compactionScheduled(false),
        fullCompactionRequested(false)
    {
    }

    void init()
    {
        if(initialized)
            return;

        // Start worker threads
        serializationThread.reset(
            new boost::thread(
                boost::bind(
                    &Impl::workerLoop,
                    shared_from_this(),
                    boost::ref(serializationQueue)
                    )
                )
            );
        compactionThread.reset(
            new boost::thread(
                boost::bind(
                    &Impl::workerLoop,
                    shared_from_this(),
                    boost::ref(compactionQueue)
                    )
                )
            );

        // Read config, load readableTables
        //   If the set of readable tables includes MemTable loads,
        //   schedule serializations for those tables.  This can be
        //   accomplished by recording the number of properly
        //   serialized tables.  Any tables read after that point
        //   should be reserialized.
        //  
        try {
            Config cfg(fs::resolve(tableDir, "state"));
            loadFromConfig(cfg);
        }
        catch(IOError const &) {
            // pass
        }
        
        // Create writable table
        {
            lock_t l(mutex);
            createWritableTable();
        }

        initialized = true;
    }

    void shutdown()
    {
        // Cancel worker queue waits
        serializationQueue.cancelWaits();
        compactionQueue.cancelWaits();

        // Join worker threads
        if(serializationThread)
        {
            serializationThread->join();
            serializationThread.reset();
        }
        if(compactionThread)
        {
            compactionThread->join();
            compactionThread.reset();
        }
    }

    void set(strref_t row, strref_t column, int64_t timestamp, strref_t value)
    {
        writableTable->set(row, column, timestamp, value);

        // If table is too big, schedule serialization
        if(memTable->getMemoryUsage() >= MAX_TABLE_MEMORY)
            startNewWritableTable();
    }

    void erase(strref_t row, strref_t column, int64_t timestamp)
    {
        writableTable->erase(row, column, timestamp);

        // If table is too big, schedule serialization
        if(memTable->getMemoryUsage() >= MAX_TABLE_MEMORY)
            startNewWritableTable();
    }
    
    CellStreamPtr scan(ScanPredicate const & pred)
    {
        // Make a merge stream, filter out erasures
        CellStreamPtr merge = CellMerge::make(true);

        // Don't use history predicate for subscans -- must add that
        // filter later
        int maxHistory = pred.getMaxHistory();
        ScanPredicate subPred(pred);
        subPred.setMaxHistory(0);

        // Lock to access readableTables
        lock_t l(mutex);

        // Block until we have a reasonable number of tables from
        // which to merge
        while(readableTables.size() > MAX_WAY_MERGE)
        {
            if(!compactionScheduled)
                scheduleCompaction(readableTables.size() - 4);
            log("scan blocking for compaction");
            compactionFinished.wait(l);
            log("blocked scan awakes");
        }

        // Add all table scans to merge, in order.  They must be added
        // to the merge in reverse order, since earlier items have
        // priority in the case of merge ties.
        for(table_list_t::const_iterator ti = readableTables.end();
            ti != readableTables.begin(); )
        {
            --ti;
            merge->pipeFrom(ti->table->scan(subPred));
        }

        // Add history filter later if we have one
        if(maxHistory)
        {
            CellStreamPtr filter = makeHistoryFilter(maxHistory);
            filter->pipeFrom(merge);
            return filter;
        }
        else
        {
            return merge;
        }
    }

    void sync()
    {
        // Sync writable table
        writableTable->sync();
    }

    void flushMemory()
    {
        if(memTable->getCellCount() > 0)
            startNewWritableTable();
    }

    void compactTable()
    {
        lock_t l(mutex);

        if(compactionScheduled)
        {
            // There's already a compaction in progress.  Make a note
            // that we want a full compaction when that's done.
            fullCompactionRequested = true;
        }
        else
        {
            // Schedule a full compaction
            scheduleFullCompaction();
        }
    }

    size_t getMemoryUsage()
    {
        return memTable->getMemoryUsage();
    }
};

//----------------------------------------------------------------------------
// LocalTable
//----------------------------------------------------------------------------
LocalTable::LocalTable(string const & tableDir) :
    impl(new Impl(tableDir))
{
    // cerr << "create: LocalTable " << (void*)this << endl;
    impl->init();
}

LocalTable::~LocalTable()
{
    impl->shutdown();
    // cerr << "delete: LocalTable " << (void*)this << endl;
}

void LocalTable::set(strref_t row, strref_t column, int64_t timestamp,
                     strref_t value)
{
    impl->set(row, column, timestamp, value);
}

void LocalTable::erase(strref_t row, strref_t column, int64_t timestamp)
{
    impl->erase(row, column, timestamp);
}

CellStreamPtr LocalTable::scan(ScanPredicate const & pred) const
{
    return impl->scan(pred);
}

void LocalTable::sync()
{
    impl->sync();
}

void LocalTable::flushMemory()
{
    impl->flushMemory();
}

void LocalTable::compactTable()
{
    impl->compactTable();
}

size_t LocalTable::getMemoryUsage() const
{
    return impl->getMemoryUsage();
}


//----------------------------------------------------------------------------
// Registration
//----------------------------------------------------------------------------
#include <warp/init.h>
#include <kdi/table_factory.h>

namespace
{
    TablePtr myOpen(string const & uri)
    {
        TablePtr p(new LocalTable(uriPopScheme(uri)));
        return p;
    }
}

WARP_DEFINE_INIT(kdi_local_local_table)
{
    TableFactory::get().registerTable("local", &myOpen);
}
