//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-03-04
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

#include <kdi/meta/meta_table.h>
#include <kdi/meta/meta_util.h>
#include <kdi/scan_predicate.h>
#include <ex/exception.h>

using namespace kdi;
using namespace kdi::meta;
using namespace warp;
using namespace ex;
using namespace std;

//----------------------------------------------------------------------------
// Util
//----------------------------------------------------------------------------
namespace {

    using kdi::meta::metaLocationScan;

    CellStreamPtr metaLocationScan(
        TablePtr const & metaTable, strref_t tableName,
        IntervalPoint<string> const & minRow)
    {
        if(minRow.isFinite())
            return metaLocationScan(metaTable, tableName, minRow.getValue());
        else
            return metaLocationScan(metaTable, tableName, "");
    }

}

//----------------------------------------------------------------------------
// MetaScanner
//----------------------------------------------------------------------------
class MetaScanner : public kdi::CellStream
{
    // The meta table.  We'll need to refer to it as we walk through
    // the scan.  Set by construction.
    TablePtr metaTable;

    // The name of the table we're scanning.  Used to index into the
    // meta table.  Set by construction.
    string tableName;

    // Scan predicate we trying to fulfill.  Set by construction.
    ScanPredicate pred;

    // Our current scan over the location column in the meta table.
    // Will initially be null.
    CellStreamPtr metaScan;

    // Our current scan of a tablet table.  Will initially be null.
    CellStreamPtr tabletScan;
    
    // The next section to scan in the row predicate.  Initially set
    // to the beginning of the row predicate.  If there is no row
    // predicate, this is undefined.
    IntervalSet<string>::const_iterator nextRow;

    // Upper row bound of the current tablet.  This advances as we
    // walk through the meta table and visit new tablets.  Initially
    // set to "").
    IntervalPoint<string> lastTabletRow;

    // Section of row predicate we're currently trying to scan.  This
    // advances as we move through the scan predicate by rows.  If
    // there is a row predicate, this is initially set to ("" "").  If
    // there is no row predicate, it is infinite.
    Interval<string> currentRowSpan;

    bool getNextTablet()
    {
        // When we scan a tablet, we get all the cells matching the
        // predicate out of the tablet, not just cells in the current
        // span.  This way, we never have to scan a tablet twice.

        // First, skip ahead in the row predicate for spans entirely
        // contained by the last tablet.  On the first time through
        // with a row predicate, both the current row span is ("" "")
        // and the lastTabletRow is ""), meaning we take the branch.
        // On the first time without a row predicate, the current row
        // span is infinite, and we won't take the branch until we're
        // at the end of the meta table.
        while(currentRowSpan.getUpperBound() <= lastTabletRow)
        {
            // If we're out of spans, we're done with the scan
            if(!pred.getRowPredicate() ||
               nextRow == pred.getRowPredicate()->end())
            {
                return false;
            }

            // Advance our current row span to the next section in the
            // row predicate
            currentRowSpan.setLowerBound(*nextRow);
            ++nextRow;
            currentRowSpan.setUpperBound(*nextRow);
            ++nextRow;
        }

        // This function is called when the current tablet is
        // exhausted.  In order to open the next tablet, we scan the
        // meta table.  If we don't have a meta scan already, we'll
        // need to make one.  If our span lower bound exceeds the
        // current tablet upper bound, we need to jump to a new
        // location in the meta table.
        if(!metaScan || currentRowSpan.getLowerBound() > lastTabletRow)
        {
            // Move our metaScan to the first cell in the meta table
            // such that spanLowerBound <= metaUpperBound
            metaScan = metaLocationScan(metaTable, tableName,
                                        currentRowSpan.getLowerBound());
        }

        // Extend into the next tablet in meta scan.
        
        // Get the next location cell in the meta table.
        Cell metaLocation;
        if(!metaScan->get(metaLocation))
        {
            // If we've reached the end of the metaScan, there are no
            // more tablets.
            return false;
        }

        // Get the upper bound from the meta cell
        lastTabletRow = getTabletRowBound(metaLocation.getRow());

        // Start the next tablet scan
        tabletScan = Table::open(str(metaLocation.getValue()))->scan(pred);

        return true;
    }

public:
    MetaScanner(TablePtr const & metaTable, string const & tableName,
                ScanPredicate const & pred) :
        metaTable(metaTable),
        tableName(tableName),
        pred(pred),
        lastTabletRow("", PT_EXCLUSIVE_UPPER_BOUND)
    {
        if(pred.getRowPredicate())
        {
            nextRow = pred.getRowPredicate()->begin();
            currentRowSpan
                .setLowerBound("", BT_EXCLUSIVE)
                .setUpperBound("", BT_EXCLUSIVE);
        }
        else
        {
            currentRowSpan.setInfinite();
        }
    }

    bool get(Cell & x)
    {
        for(;;)
        {
            // If the current scan is valid and it has a cell for us,
            // we're done.
            if(tabletScan && tabletScan->get(x))
                return true;

            // Otherwise, we need to advance the currentScan to the
            // next segment.
            if(!getNextTablet())
                return false;
        }
    }
};


//----------------------------------------------------------------------------
// MetaTable
//----------------------------------------------------------------------------
MetaTable::MetaTable(TablePtr const & metaTable, string const & tableName) :
    metaCache(metaTable),
    tableName(tableName)
{
    lastRows.setEmpty();
    lastIt = tabletCache.end();
}

void MetaTable::set(strref_t row, strref_t column, int64_t timestamp,
         strref_t value)
{
    getTablet(row)->set(row,column,timestamp,value);
}

void MetaTable::erase(strref_t row, strref_t column, int64_t timestamp)
{
    getTablet(row)->erase(row,column,timestamp);
}

void MetaTable::sync()
{
    // The syncs could be issued in parallel with a thread pool
    for(locmap_t::iterator i = tabletCache.begin();
        i != tabletCache.end(); ++i)
    {
        entry_t & ent = i->second;
        if(ent.second)
        {
            ent.first->sync();
            ent.second = false;
        }
    }
}

CellStreamPtr MetaTable::scan() const
{
    return scan(ScanPredicate());
}

CellStreamPtr MetaTable::scan(ScanPredicate const & pred) const
{
    CellStreamPtr p(
        new MetaScanner(
            metaCache.getMetaTable(),
            tableName,
            pred
            )
        );
    return p;
}


//----------------------------------------------------------------------------
// Registration
//----------------------------------------------------------------------------
#include <warp/init.h>
#include <warp/uri.h>
#include <kdi/table_factory.h>

namespace
{
    TablePtr myOpen(string const & uri)
    {
        string tableName = uriDecode(uriGetParameter(uri, "name"), true);
        if(tableName.empty())
            raise<ValueError>("meta table requires 'name' parameter");

        string metaUri = uriPopScheme(uriEraseParameter(uri, "name"));
        TablePtr metaTable = Table::open(metaUri);
        
        TablePtr p(new MetaTable(metaTable, tableName));
        return p;
    }
}

WARP_DEFINE_INIT(kdi_meta_meta_table)
{
    TableFactory::get().registerTable("meta", &myOpen);
}
