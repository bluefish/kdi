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
#include <kdi/marshal/cell_block.h>
#include <kdi/marshal/cell_block_builder.h>
#include <warp/builder.h>
#include <warp/uri.h>
#include <warp/log.h>
#include <warp/fs.h>
#include <ex/exception.h>

#include <kdi/net/TableManager.h>

#include <boost/scoped_ptr.hpp>
#include <boost/format.hpp>
#include <Ice/Ice.h>

using namespace kdi;
using namespace kdi::net;
using namespace warp;
using namespace ex;
using namespace std;

using boost::format;

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
}

//----------------------------------------------------------------------------
// NetTable::Scanner
//----------------------------------------------------------------------------
class NetTable::Scanner
    : public kdi::CellStream
{
    boost::shared_ptr<Impl const> table;
    ScanPredicate pred;

    details::ScannerPrx scanner;
    
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

    boost::scoped_ptr<Key> lastKey;
    
    Ice::ByteSeq buffer;
    kdi::marshal::CellData const * next;
    kdi::marshal::CellData const * end;
    bool eof;

    bool refillBuffer()
    {
        using kdi::marshal::CellBlock;

        size_t const MAX_FAILURES = 5;
        size_t scanFailures = 0;
        for(;;)
        {
            if(eof)
                return false;

            try {
                scanner->getBulk(buffer, eof);
            }
            catch(Ice::RequestFailedException const &) {
                // Only retry so much
                if(++scanFailures >= MAX_FAILURES)
                    throw;

                // If request fails, reopen the scan an try again
                reopen();
                continue;
            }
            
            assert(!buffer.empty());
            CellBlock const * block =
                reinterpret_cast<CellBlock const *>(&buffer[0]);

            // cerr << format("gotBulk: %d cells, %d bytes, eof=%s")
            //     % block->cells.size() % buffer.size() % eof
            //      << endl;

            next = block->cells.begin();
            end = block->cells.end();

            // If we've set lastKey, it's the key of the last cell we
            // returned in this scan.  We need to advance the scan
            // until it is past lastKey.
            if(lastKey)
            {
                for(; next != end; ++next)
                {
                    if(*lastKey < next->key)
                    {
                        lastKey.reset();
                        break;
                    }
                }
            }

            if(next != end)
                return true;
        }
    }
    
    void setLastKey()
    {
        // Already set?
        if(lastKey)
            return;

        // Haven't returned any cells yet?
        if(!next)
            return;

        // Get the block to make sure we're not at the beginning of
        // it.
        using kdi::marshal::CellBlock;
        CellBlock const * block = 
            reinterpret_cast<CellBlock const *>(&buffer[0]);

        // Haven't returned any cells yet?
        if(next == block->cells.begin())
            return;

        // Init lastKey from most recently returned cell
        lastKey.reset(new Key(next[-1].key));

        // If we have a positive history predicate, let's just advance
        // to the end of this (row,column) set.
        if(pred.getMaxHistory() > 0)
        {
            // Pretend like we've already returned the last possible
            // timestamp so we skip everything in the same time-cell.
            lastKey->timestamp = std::numeric_limits<int64_t>::min();
        }
    }
    
    void reopen();

public:
    explicit Scanner(boost::shared_ptr<Impl const> const & table, ScanPredicate const & pred) :
        table(table),
        pred(pred),
        next(0),
        end(0),
        eof(false)
    {
        reopen();
    }

    ~Scanner()
    {
        try {
            scanner->close();
        }
        catch(Ice::RequestFailedException const & ex) {
            // Don't worry about it.
        }
        catch(Ice::Exception const & ex) {
            log("ERROR on scanner close: %s", ex);
        }
    }

    bool get(Cell & x)
    {
        if(next == end && !refillBuffer())
            return false;

        x = makeCell(*next->key.row, *next->key.column,
                     next->key.timestamp, *next->value);
        ++next;
        return true;
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
            table->applyMutations(buffer);
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

        if(!ua.host || !ua.port)
            raise<RuntimeError>("need remote host and port: %s", uri);
        
        try {
            // Get TableManager
            details::TableManagerPrx mgr =
                details::TableManagerPrx::checkedCast(
                    getCommunicator()->stringToProxy(
                        (format("TableManager:tcp -h %s -p %s")
                         % ua.host % ua.port).str()
                        )
                    );

            // Get Table
            table = mgr->openTable(fs::path(uri));
        }
        catch(Ice::Exception const & ex) {
            raise<RuntimeError>("couldn't open net table %s: %s", uri, ex);
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

    CellStreamPtr scan() const
    {
        return scan(ScanPredicate());
    }

    CellStreamPtr scan(ScanPredicate const & pred) const
    {
        //log("NetTable::scan(%s)", pred);
        CellStreamPtr ptr(new Scanner(shared_from_this(), pred));
        return ptr;
    }

    details::ScannerPrx getScanner(ScanPredicate const & pred) const
    {
        //log("NetTable::getScanner(%s)", pred);
        std::ostringstream oss;
        oss << pred;
        return table->scan(oss.str());
    }

    void sync()
    {
        flush();
        table->sync();
    }
};

//----------------------------------------------------------------------------
// NetTable::Scanner
//----------------------------------------------------------------------------
void NetTable::Scanner::reopen()
{
    // The the last key if we can.
    setLastKey();

    // Trim the scan predicate if we have a last cell
    ScanPredicate p;
    if(lastKey)
        p = pred.clipRows(makeLowerBound(lastKey->row));
    else
        p = pred;

    // Reopen the scanner
    scanner = table->getScanner(p);
}


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

CellStreamPtr NetTable::scan() const
{
    return impl->scan();
}

CellStreamPtr NetTable::scan(ScanPredicate const & pred) const
{
    return impl->scan(pred);
}

void NetTable::sync()
{
    impl->sync();
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
