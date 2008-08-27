//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-08-21
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

// Silly Hypertable
#include <Common/Compat.h>

#include <kdi/hyper/Table.h>
#include <kdi/hyper/Client.h>
#include <kdi/scan_predicate.h>
#include <kdi/cell_filter.h>
#include <warp/base64.h>
#include <vector>

//#include <warp/log.h>
//#include <warp/timestamp.h>
//using warp::log;
//using warp::Timestamp;

using kdi::strref_t;
using namespace ex;

//----------------------------------------------------------------------------
// Util
//----------------------------------------------------------------------------
namespace
{
    char const * const ASCII_STABLE = 
        "+/0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    warp::Base64Encoder const & getEncoder()
    {
        static warp::Base64Encoder const ENCODER(ASCII_STABLE);
        return ENCODER;
    }
    
//----------------------------------------------------------------------------
// HtKey
//----------------------------------------------------------------------------
    class HtKey : public ::Hypertable::KeySpec
    {
        std::vector<char> encodedRow;
        std::vector<char> encodedCol;

    public:
        HtKey(strref_t row, strref_t col, int64_t timestamp)
        {
            uint64_t t(timestamp ^
                       std::numeric_limits<int64_t>::max());

            //log("encode t: orig=0x%016x(%s) enc=0x%016x",
            //    timestamp, Timestamp::fromMicroseconds(timestamp),
            //    t);

            BOOST_STATIC_ASSERT(sizeof(t) == 8);
            std::vector<char> tmp(col.size() + sizeof(t));
            memcpy(&tmp[0], col.begin(), col.size());
            char * p = &tmp[col.size()];
            p[0] = char((t >> 56) & 0xff);
            p[1] = char((t >> 48) & 0xff);
            p[2] = char((t >> 40) & 0xff);
            p[3] = char((t >> 32) & 0xff);
            p[4] = char((t >> 24) & 0xff);
            p[5] = char((t >> 16) & 0xff);
            p[6] = char((t >>  8) & 0xff);
            p[7] = char((t      ) & 0xff);

            getEncoder().encode(tmp, encodedCol);
            getEncoder().encode(row, tmp);
            tmp.swap(encodedRow);

            //log("col: %s", warp::StringRange(encodedCol));

            encodedRow.push_back('\0');
            encodedCol.push_back('\0');

            this->row = &encodedRow[0];
            this->row_len = encodedRow.size() - 1;
            this->column_family = "X";
            this->column_qualifier = &encodedCol[0];
            this->column_qualifier_len = encodedCol.size() - 1;
        }
    };
    
//----------------------------------------------------------------------------
// addRowIntervals
//----------------------------------------------------------------------------
    void addRowIntervals(::Hypertable::ScanSpecBuilder & spec,
                         warp::IntervalSet<std::string> const & rows)
    {
        using std::string;
        using warp::IntervalSet;
        using warp::IntervalPoint;

        for(IntervalSet<string>::const_iterator i = rows.begin();
            i != rows.end(); )
        {
            IntervalPoint<string> const & lo = *i; ++i;
            IntervalPoint<string> const & hi = *i; ++i;

                string loV, hiV;
                bool loI, hiI;
                
                if(lo.isInfinite())
                {
                    loV = "";
                    loI = true;
                }
                else
                {
                    getEncoder().encode(lo.getValue(), loV);
                    loI = lo.isInclusive();
                }

                if(hi.isInfinite())
                {
                    hiV = "~";
                    hiI = true;
                }
                else
                {
                    getEncoder().encode(hi.getValue(), hiV);
                    hiI = hi.isInclusive();
                }

                spec.add_row_interval(loV, loI, hiV, hiI);
        }
    }

//----------------------------------------------------------------------------
// rethrow
//----------------------------------------------------------------------------
    EX_NORETURN void rethrow(::Hypertable::Exception const & ex)
    {
        raise<RuntimeError>(
            "Hypertable exception: %s (code=%d func=%s loc=%s:%d) %s",
            ::Hypertable::Error::get_text(ex.code()),
            ex.code(),
            ex.func(),
            ex.file(),
            ex.line(),
            ex.what());
    }

//----------------------------------------------------------------------------
// decodeCell
//----------------------------------------------------------------------------
    inline kdi::Cell decodeCell(::Hypertable::Cell const & x)
    {
        static warp::Base64Decoder const DECODER(ASCII_STABLE);

        std::vector<char> row;
        DECODER.decode(x.row_key, row);

        //log("col: %s", warp::StringRange(x.column_qualifier));

        std::vector<char> col;
        DECODER.decode(x.column_qualifier, col);

        BOOST_STATIC_ASSERT(sizeof(uint64_t) == 8);
        if(col.size() < sizeof(uint64_t))
            raise<RuntimeError>("corrupted column encoding");
        char const * p = &col[col.size() - sizeof(uint64_t)];
        uint64_t t = ( ((uint64_t(p[0]) & 0xff) << 56) |
                       ((uint64_t(p[1]) & 0xff) << 48) |
                       ((uint64_t(p[2]) & 0xff) << 40) |
                       ((uint64_t(p[3]) & 0xff) << 32) |
                       ((uint64_t(p[4]) & 0xff) << 24) |
                       ((uint64_t(p[5]) & 0xff) << 16) |
                       ((uint64_t(p[6]) & 0xff) <<  8) |
                       ((uint64_t(p[7]) & 0xff)      ) );
        int64_t timestamp = int64_t(t) ^ std::numeric_limits<int64_t>::max();
        col.erase(col.end() - sizeof(uint64_t), col.end());

        //log("decode t: recv=0x%016x(%s) enc=0x%016x",
        //    timestamp, Timestamp::fromMicroseconds(timestamp),
        //    t);

        return kdi::makeCell(
            row, col, timestamp,
            warp::StringRange(x.value, x.value_len)
            );
    }
}

//----------------------------------------------------------------------------
// Scanner
//----------------------------------------------------------------------------
namespace
{
    class Scanner
        : public kdi::CellStream
    {
        ::Hypertable::TableScannerPtr scanner;

    public:
        explicit Scanner(::Hypertable::TableScannerPtr const & scanner) :
            scanner(scanner)
        {
        }

        bool get(kdi::Cell & x)
        {
            try {
                ::Hypertable::Cell cell;
                if(!scanner->next(cell))
                    return false;

                x = decodeCell(cell);
                return true;
            }
            catch(::Hypertable::Exception const & ex) {
                rethrow(ex);
            }
        }
    };
}


//----------------------------------------------------------------------------
// Table
//----------------------------------------------------------------------------
kdi::hyper::Table::Table(std::string const & tableName)
{
    try {
        Hypertable::ClientPtr client = getClient();
        table = client->open_table(tableName);
    }
    catch(::Hypertable::Exception const & ex) {
        rethrow(ex);
    }
}

void kdi::hyper::Table::set(
    strref_t row, strref_t column, int64_t timestamp, strref_t value)
{
    try {
        if(!mutator)
            mutator = table->create_mutator();

        HtKey key(row, column, timestamp);
        mutator->set(key, value.begin(), value.size());
    }
    catch(::Hypertable::Exception const & ex) {
        rethrow(ex);
    }
}

void kdi::hyper::Table::erase(
    strref_t row, strref_t column, int64_t timestamp)
{
    try {
        if(!mutator)
            mutator = table->create_mutator();

        HtKey key(row, column, timestamp);
        mutator->set_delete(key);
    }
    catch(::Hypertable::Exception const & ex) {
        rethrow(ex);
    }
}

void kdi::hyper::Table::sync()
{
    try {
        if(mutator)
        {
            mutator->flush();
            mutator = 0;
        }
    }
    catch(::Hypertable::Exception const & ex) {
        rethrow(ex);
    }
}

kdi::CellStreamPtr kdi::hyper::Table::scan() const
{
    return scan(ScanPredicate());
}

kdi::CellStreamPtr kdi::hyper::Table::scan(
    ScanPredicate const & pred) const
{
    try {
        ::Hypertable::ScanSpecBuilder spec;

        spec.add_column("X");
        spec.set_return_deletes(false);
        spec.set_max_versions(1);

        if(ScanPredicate::StringSetCPtr rows = pred.getRowPredicate())
            addRowIntervals(spec, *rows);

        ::Hypertable::TableScannerPtr scanner =
              table->create_scanner(spec.get());

        // Assume that HT will do the row filtering for us
        CellStreamPtr p(new Scanner(scanner));
        return applyPredicateFilter(
            ScanPredicate(pred).clearRowPredicate(),
            p);
    }
    catch(::Hypertable::Exception const & ex) {
        rethrow(ex);
    }
}


//----------------------------------------------------------------------------
// Reg
//----------------------------------------------------------------------------
#include <warp/init.h>
#include <warp/uri.h>
#include <kdi/table_factory.h>

namespace
{
    kdi::TablePtr openHyper(std::string const & uri)
    {
        warp::Uri u(uri);
        kdi::TablePtr p(new kdi::hyper::Table(u.path.toString()));
        return p;
    }
}

WARP_DEFINE_INIT(kdi_hyper_table)
{
    kdi::TableFactory::get().registerTable(
        "hyper", &openHyper
        );
}
