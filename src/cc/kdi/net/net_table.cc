//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-12
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

#include <kdi/net/net_table.h>
#include <kdi/scan_predicate.h>
#include <kdi/RowInterval.h>
#include <kdi/row_stream.h>
#include <kdi/marshal/cell_block.h>
#include <kdi/marshal/cell_block_builder.h>
#include <kdi/meta/meta_util.h>
#include <warp/builder.h>
#include <warp/uri.h>
#include <warp/log.h>
#include <warp/fs.h>
#include <ex/exception.h>

#include <kdi/net/TableManager.h>

#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <boost/format.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/xtime.hpp>
#include <Ice/Ice.h>
#include <IceUtil/Handle.h>
#include <sstream>
#include <queue>
#include <time.h>

using namespace kdi;
using namespace kdi::net;
using namespace warp;
using namespace ex;
using namespace std;

using boost::format;

namespace {

    bool shouldAutosync()
    {
        static int autoSync = -1;
        if(autoSync < 0)
        {
            char * s = getenv("KDI_NOAUTOSYNC");
            if(s && *s)
                autoSync = 0;
            else
                autoSync = 1;
        }
        return autoSync != 0;
    }

}

#undef DLOG
#define DLOG(x) ((void) 0)

#undef DLOG2
#define DLOG2(x,y) ((void) 0)

namespace
{
    Ice::CommunicatorPtr const & getCommunicator()
    {
        int ac = 2;
        char const * av[] = { "foo", "--Ice.MessageSizeMax=32768" };

        static Ice::CommunicatorPtr com(
            Ice::initialize(ac, const_cast<char **>(av)));
        return com;
    }

    int const RETRY_WAIT_SECONDS = 60;
    int const MAX_CONNECTION_ATTEMPTS = 15;   // 15 minutes

    size_t const MAX_SCAN_FAILURES = 15;
    size_t const N_SCAN_BUFFERS = 3;

    boost::xtime make_xtime(time_t t)
    {
        boost::xtime xt;
        boost::xtime_get(&xt, boost::TIME_UTC);
        xt.sec += t - time(0);
        return xt;
    }
}


//----------------------------------------------------------------------------
// MetaIntervalScanner
//----------------------------------------------------------------------------
namespace
{
    class MetaIntervalScanner
        : public RowIntervalStream
    {
        RowStream metaRows;
        IntervalPoint<string> lowerBound;
        bool returnedAtLeastOne;

    public:
        explicit MetaIntervalScanner(CellStreamPtr const & metaScan) :
            metaRows(metaScan),
            lowerBound("", PT_INCLUSIVE_LOWER_BOUND),
            returnedAtLeastOne(false)
        {
        }

        bool get(RowInterval & x)
        {
            Cell cell;
            if(metaRows.fetch() && metaRows.get(cell))
            {
                x.setLowerBound(lowerBound);
                x.setUpperBound(kdi::meta::getTabletRowBound(cell.getRow()));
            }
            else if(!returnedAtLeastOne)
            {
                x.setLowerBound(lowerBound);
                x.unsetUpperBound();
            }
            else
            {
                return false;
            }

            if(x.getUpperBound().isFinite())
                lowerBound = x.getUpperBound().getAdjacentComplement();

            returnedAtLeastOne = true;
            return true;
        }
    };
}


//----------------------------------------------------------------------------
// NetTable::Scanner
//----------------------------------------------------------------------------
class NetTable::Scanner
    : public kdi::CellStream
{
public:
    Scanner(details::TablePrx const & table, ScanPredicate const & pred) :
        table(table),
        pred(pred),
        buffers(new Buffer[N_SCAN_BUFFERS]),
        activeBuf(0),
        lastBuf(0),
        nFailures(0),
        delayUntil(0),
        eos(false),
        pending(false),
        scannerOpen(false)
    {
        for(size_t i = 0; i < N_SCAN_BUFFERS; ++i)
            emptyQ.push(&buffers[i]);
    }

    ~Scanner()
    {
        lock_t lock(mutex);
        eos = true;
        while(pending || scannerOpen)
        {
            maybeStartClose_locked();
            DLOG("~ Wait");
            cond.wait(lock);
            DLOG("~ Wait: wakeup");
        }
    }

    bool get(Cell & x)
    {
        if(!activeBuf && !nextBuffer())
            return false;

        activeBuf->get(x);

        if(activeBuf->empty())
            releaseBuffer();

        return true;
    }

private:
    /// Key of a fetched cell
    struct Key
    {
        std::string row;
        std::string column;
        int64_t timestamp;

        Key(kdi::marshal::CellKey const & key) :
            row(key.row->begin(), key.row->end()),
            column(key.column->begin(), key.column->end()),
            timestamp(key.timestamp)
        {
        }

        bool operator<(kdi::marshal::CellKey const & o) const
        {
            if(int cmp = string_compare(row, *o.row))
                return cmp < 0;
            else if(int cmp = string_compare(column, *o.column))
                return cmp < 0;
            else
                return o.timestamp < timestamp;
        }
    };

    /// Buffer representing one block from getBulk()
    class Buffer
    {
        Ice::ByteSeq buffer;
        kdi::marshal::CellData const * next;
        kdi::marshal::CellData const * end;

    public:
        Buffer() : next(0), end(0) {}

        void set(Ice::ByteSeq const & other, ptrdiff_t firstIdx)
        {
            buffer = other;

            using kdi::marshal::CellBlock;
            CellBlock const * block =
                reinterpret_cast<CellBlock const *>(&buffer[0]);

            assert(firstIdx >= 0 && size_t(firstIdx) < block->cells.size());

            next = block->cells.begin() + firstIdx;
            end = block->cells.end();
        }

        void get(Cell & x)
        {
            x = makeCell(*next->key.row, *next->key.column,
                         next->key.timestamp, *next->value);
            ++next;
        }

        kdi::marshal::CellKey const &
        getLastKey() const { return end[-1].key; }

        bool empty() const { return next == end; }
    };

    enum OpType { OP_SCAN, OP_FETCH, OP_CLOSE };

    /// Callback for Scanner.getBulk
    struct FetchCb : public details::AMI_Scanner_getBulk
    {
        Scanner * scanner;
        FetchCb(Scanner * scanner) : scanner(scanner) {}
        void ice_response(Ice::ByteSeq const & cells, bool lastBlock) {
            scanner->handleFetch(cells, lastBlock);
        }
        void ice_exception(Ice::Exception const & ex) {
            scanner->handleError(ex, OP_FETCH);
        }
    };

    /// Callback for Table.scan
    struct ScanCb : public details::AMI_Table_scan
    {
        Scanner * scanner;
        ScanCb(Scanner * scanner) : scanner(scanner) {}
        void ice_response(details::ScannerPrx const & prx) {
            scanner->handleScan(prx);
        }
        void ice_exception(Ice::Exception const & ex) {
            scanner->handleError(ex, OP_SCAN);
        }
    };

    /// Callback for Scanner.close
    struct CloseCb : public details::AMI_Scanner_close
    {
        Scanner * scanner;
        CloseCb(Scanner * scanner) : scanner(scanner) {}
        void ice_response() {
            scanner->handleClose();
        }
        void ice_exception(Ice::Exception const & ex) {
            scanner->handleError(ex, OP_CLOSE);
        }
    };

private:
    // Table proxy for scan, given at creation
    details::TablePrx const table;

    // Original predicate for scan, given at creation
    ScanPredicate const pred;

    // Current Scanner proxy, valid if scannerOpen == true
    details::ScannerPrx scanner;

    // The last key fetched.  If we have to reopen the scanner, resume
    // after this point.
    boost::scoped_ptr<Key> lastKey;

    // Storage for buffers
    boost::scoped_array<Buffer> buffers;

    // Stored exception from an async call
    boost::scoped_ptr<Ice::Exception> error;

    // The active buffer, only accessed by client thread
    Buffer * activeBuf;

    // The last buffer fetched
    Buffer * lastBuf;

    // Queue of filled buffers
    std::queue<Buffer *> readyQ;

    // Queue of empty buffers
    std::queue<Buffer *> emptyQ;

    // Count of successive errors that have occurred.
    size_t nFailures;

    // If an error occurs, delay next async call until this time
    time_t delayUntil;

    // Indicates end-of-stream.  Set if we've reached the end or
    // encounter a persistent error.
    bool eos;

    // Indicates there is an async call pending.
    bool pending;

    // Indicates we have an open scanner on a remote server and we
    // should eventually call close() on it.
    bool scannerOpen;

    // Synchronization mutex
    boost::mutex mutex;

    // Condition to signal async operation is complete
    boost::condition cond;

    typedef boost::mutex::scoped_lock lock_t;

private:
    /// Wait for a full buffer and make it the active buffer.  Return
    /// true if we can get one.
    bool nextBuffer()
    {
        lock_t lock(mutex);

        while(readyQ.empty() && !eos)
        {
            if(time(0) < delayUntil)
            {
                DLOG("Timed wait");
                cond.timed_wait(lock, make_xtime(delayUntil));
                DLOG("Timed wait: wakeup");
            }
            else
            {
                maybeStartFetch_locked();
                DLOG("Wait");
                cond.wait(lock);
                DLOG("Wait: wakeup");
            }
        }

        if(readyQ.empty())
        {
            if(error)
                error->ice_throw();

            DLOG("eos");

            return false;
        }
        
        activeBuf = readyQ.front();
        readyQ.pop();

        DLOG("got");

        return true;
    }

    /// Release active buffer into the empty queue.
    void releaseBuffer()
    {
        lock_t lock(mutex);

        emptyQ.push(activeBuf);
        activeBuf = 0;

        maybeStartFetch_locked();
    }

    /// If we can start an asynchronous getBulk() call, go ahead and
    /// do so.
    void maybeStartFetch_locked()
    {
        DLOG("maybeStartFetch");

        // If we're at EOS, there's already a pending call, or there's
        // nothing to fetch into, don't start a new fetch call
        if(eos || pending || emptyQ.empty())
        {
            DLOG("no");
            return;
        }

        // We need a scanner before we can fetch
        if(!scannerOpen)
        {
            // Set the last key if we can.
            setLastKey_locked();

            // Trim the scan predicate if we have a last cell
            ScanPredicate p;
            if(lastKey)
                p = pred.clipRows(makeLowerBound(lastKey->row));
            else
                p = pred;

            // Open the scanner
            ostringstream oss;
            oss << p;
            table->scan_async(new ScanCb(this), oss.str());
            pending = true;
            
            DLOG("started scan()");

            // Don't start a fetch -- wait for the scan to open
            return;
        }

        // Start a fetch
        scanner->getBulk_async(new FetchCb(this));
        pending = true;

        DLOG("started getBulk()");
    }

    void maybeStartClose_locked()
    {
        DLOG("maybeStartClose");

        // Don't close if there's no scanner open or we have a pending
        // call
        if(!scannerOpen || pending)
        {
            DLOG("no");
            return;
        }

        // Close the scanner
        scanner->close_async(new CloseCb(this));
        pending = true;

        DLOG("started close()");
    }

    /// Set the last key from the last buffer we've successfully
    /// fetched.  This happens when we're about to open a new scanner.
    void setLastKey_locked()
    {
        // If we've already set it, don't bother resetting it.
        if(lastKey)
            return;

        // Don't set the last key if we haven't fetched any cells yet.
        if(!lastBuf)
            return;

        // Init lastKey from last fetched cell
        lastKey.reset(new Key(lastBuf->getLastKey()));

        // If we have a positive history predicate, let's just advance
        // to the end of this (row,column) set.
        if(pred.getMaxHistory() > 0)
        {
            // Pretend like we've already returned the last possible
            // timestamp so we skip everything in the same time-cell.
            lastKey->timestamp = std::numeric_limits<int64_t>::min();
        }
    }

    /// Called when Table.scan_async() completes successfully
    void handleScan(details::ScannerPrx const & prx)
    {
        DLOG("handleScan");

        lock_t lock(mutex);
        pending = false;
        nFailures = 0;

        // We have a scanner
        scanner = prx;
        scannerOpen = true;

        // Start fetching
        maybeStartFetch_locked();

        // If the fetch didn't start for some reason, notify main
        // thread it should wake up.  One possible reason: destructor
        // was called, setting EOS flag.
        if(!pending)
            cond.notify_one();
    }

    /// Called when Scanner.getBulk_async() completes successfully
    void handleFetch(Ice::ByteSeq const & buffer, bool lastCell)
    {
        DLOG("handleFetch");

        lock_t lock(mutex);
        pending = false;
        nFailures = 0;

        // Get the CellBlock from the buffer
        using kdi::marshal::CellBlock;
        CellBlock const * block =
            reinterpret_cast<CellBlock const *>(&buffer[0]);
        
        kdi::marshal::CellData const * next = block->cells.begin();
        kdi::marshal::CellData const * end = block->cells.end();

        // If we've set lastKey, it's the key of the last cell we've
        // fetched and queued.  We need to advance the scan until it
        // is past lastKey.
        if(lastKey)
        {
            for(; next != end; ++next)
            {
                if(*lastKey < next->key)
                {
                    // Done.  Clear last key.
                    lastKey.reset();
                    break;
                }
            }
        }

        // Should we wake the main thread?
        bool shouldSignal = false;

        // If we still have some cells left in the buffer, put the
        // buffer in the ready queue
        if(next != end)
        {
            Buffer * b = emptyQ.front();
            emptyQ.pop();
            
            b->set(buffer, next - block->cells.begin());
            readyQ.push(b);

            // Set the pointer for the last buffer fetched
            lastBuf = b;

            // Notify others that new data is available
            shouldSignal = true;
        }

        // Did we reach the end of the stream?
        if(lastCell || eos)
        {
            // Set the end-of-stream flag
            eos = true;

            // Notify others that we've reached the end of the stream
            shouldSignal = true;

            // Close the scanner
            maybeStartClose_locked();
        }
        else
        {
            // Get some more data if we can
            maybeStartFetch_locked();
        }

        // Maybe signal
        if(shouldSignal)
            cond.notify_one();
    }

    /// Called when Scanner.close_async() completes successfully.
    void handleClose()
    {
        DLOG("handleClose");

        lock_t lock(mutex);
        pending = false;
        nFailures = 0;

        // Scanner is closed
        scannerOpen = false;
        cond.notify_one();
    }

    /// Called if any of the async operations encounters an error.
    void handleError(Ice::Exception const & ex, OpType op)
    {
        DLOG2("handleError %s",
              op == OP_FETCH ? "FETCH" :
              op == OP_SCAN ? "SCAN" :
              op == OP_CLOSE ? "CLOSE" :
              "<UNKNOWN>");

        lock_t lock(mutex);
        pending = false;
        ++nFailures;

        // If we get an error, consider the scanner closed
        scannerOpen = false;

        // How to handle this error?
        int retryWait = RETRY_WAIT_SECONDS;
        bool reportError = true;

        // Don't ever retry a close
        if(op == OP_CLOSE)
            nFailures = MAX_SCAN_FAILURES;

        // We're willing to retry some types of errors
        try {
            ex.ice_throw();
        }
        catch(Ice::ObjectNotExistException const &) {
            // Only log errors when table is missing
            reportError = (op == OP_SCAN);

            // Don't retry if table is missing
            if(op == OP_SCAN)
                nFailures = MAX_SCAN_FAILURES;

            // Don't wait to reopen fetch
            else if(op == OP_FETCH)
                retryWait = 0;
        }
        catch(Ice::RequestFailedException const &)  {}
        catch(Ice::SocketException const &)         {}
        catch(Ice::TimeoutException const &)        {}
        catch(...)                                  { nFailures = MAX_SCAN_FAILURES; }

        if(reportError)
        {
            log("Scanner error on %s: %s",
                op == OP_FETCH ? "FETCH" :
                op == OP_SCAN ? "SCAN" :
                op == OP_CLOSE ? "CLOSE" :
                "<UNKNOWN>",
                ex);
        }

        // Have we reached the maximum error count?
        if(nFailures >= MAX_SCAN_FAILURES)
        {
            // Forward the error to the client thread (suppress all
            // close errors)
            if(op != OP_CLOSE)
                error.reset(ex.ice_clone());

            // Mark the end-of-stream
            eos = true;
        }
        else if(retryWait)
        {
            // We'll try again later.  Set a delay time.
            delayUntil = time(0) + retryWait;

            log("Will retry in %d seconds (attempt %d of %d)",
                retryWait, nFailures, MAX_SCAN_FAILURES);
        }

        cond.notify_one();
    }
};


//----------------------------------------------------------------------------
// NetTable::Impl
//----------------------------------------------------------------------------
class NetTable::Impl
    : public boost::enable_shared_from_this<NetTable::Impl>,
      private boost::noncopyable
{
    enum { FLUSH_THRESHOLD = 100 << 10 };  // 100k

    std::string uri;
    details::TablePrx table;

    warp::Builder builder;
    kdi::marshal::CellBlockBuilder cellBuilder;
    Ice::ByteSeq buffer;

    void reset()
    {
        builder.reset();
        cellBuilder.reset();
    }

    void flush()
    {
        if(cellBuilder.getCellCount() > 0)
        {
            // cerr << format("Apply: %d cells, %d est. bytes")
            //     % cellBuilder.getCellCount() % cellBuilder.getDataSize()
            //      << endl;

            builder.finalize();
            buffer.resize(builder.getFinalSize());

            // cerr << format("  final size: %d bytes") % buffer.size()
            //      << endl;

            builder.exportTo(&buffer[0]);
            reset();

            for(int attempt = 0;;)
            {
                try {
                    table->applyMutations(buffer);
                    if(shouldAutosync())
                        table->sync();
                    break;
                }
                catch(Ice::SocketException const & ex) {
                    log("connection error on %s: %s", uri, ex);
                }
                catch(Ice::TimeoutException const & ex) {
                    log("timeout error on %s: %s", uri, ex);
                }

                if(++attempt >= MAX_CONNECTION_ATTEMPTS)
                    raise<RuntimeError>("lost connection to %s", uri);

                int sleepTime = RETRY_WAIT_SECONDS;
                log("will retry in %d seconds (attempt %d of %d)",
                    sleepTime, attempt, MAX_CONNECTION_ATTEMPTS);

                sleep(sleepTime);
            }
        }
    }

    void maybeFlush()
    {
        if(cellBuilder.getDataSize() >= FLUSH_THRESHOLD)
            flush();
    }

    void reopen()
    {
        Uri u(wrap(uri));
        UriAuthority ua(u.authority);

        std::string host("localhost");
        if(ua.host)
            host.assign(ua.host.begin(), ua.host.end());

        std::string port("34177");
        if(ua.port)
            port.assign(ua.port.begin(), ua.port.end());

        try {
            // Get TableManager
            details::TableManagerPrx mgr =
                details::TableManagerPrx::checkedCast(
                    getCommunicator()->stringToProxy(
                        (format("TableManager:tcp -h %s -p %s")
                         % host % port).str()
                        )
                    );

            // Get Table
            table = mgr->openTable(fs::path(uri));
        }
        catch(Ice::Exception const & ex) {
            raise<RuntimeError>("couldn't open net table %s (%s:%s): %s",
                                uri, host, port, ex);
        }
    }

public:
    explicit Impl(string const & uri) :
        uri(uri),
        cellBuilder(&builder)
    {
        reopen();
    }

    ~Impl()
    {
        flush();
    }

    void set(strref_t row, strref_t column, int64_t timestamp,
             strref_t value)
    {
        cellBuilder.appendCell(row, column, timestamp, value);
        maybeFlush();
    }

    void erase(strref_t row, strref_t column, int64_t timestamp)
    {
        cellBuilder.appendErasure(row, column, timestamp);
        maybeFlush();
    }

    CellStreamPtr scan(ScanPredicate const & pred) const
    {
        //log("NetTable::scan(%s)", pred);
        CellStreamPtr ptr(new Scanner(table, pred));
        return ptr;
    }

    void sync()
    {
        flush();
    }

    RowIntervalStreamPtr scanIntervals() const
    {
        TablePtr metaTable(
            new NetTable(fs::replacePath(uri, "META"))
            );

        string name = fs::path(uri);
        if(!name.empty() && name[0] == '/')
            name = name.substr(1);

        CellStreamPtr metaScan = kdi::meta::metaScan(metaTable, name);
        RowIntervalStreamPtr p(
            new MetaIntervalScanner(metaScan)
            );
        return p;
    }

};


//----------------------------------------------------------------------------
// NetTable
//----------------------------------------------------------------------------
NetTable::NetTable(string const & uri) :
    impl(new Impl(uri))
{
}

NetTable::~NetTable()
{
}

void NetTable::set(strref_t row, strref_t column, int64_t timestamp,
                   strref_t value)
{
    impl->set(row, column, timestamp, value);
}

void NetTable::erase(strref_t row, strref_t column, int64_t timestamp)
{
    impl->erase(row, column, timestamp);
}

CellStreamPtr NetTable::scan(ScanPredicate const & pred) const
{
    return impl->scan(pred);
}

void NetTable::sync()
{
    impl->sync();
}

RowIntervalStreamPtr NetTable::scanIntervals() const
{
    return impl->scanIntervals();
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
        TablePtr p(new NetTable(uriPopScheme(uri)));
        return p;
    }
}

WARP_DEFINE_INIT(kdi_net_net_table)
{
    TableFactory::get().registerTable("kdi", &myOpen);
}
