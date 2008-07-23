//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-07-22
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

#include <kdi/tablet/SuperTablet.h>
#include <kdi/tablet/Tablet.h>
#include <kdi/tablet/TabletName.h>
#include <kdi/tablet/TabletConfig.h>
#include <kdi/scan_predicate.h>
#include <warp/interval.h>
#include <warp/log.h>
#include <boost/format.hpp>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace ex;
using namespace std;
using boost::format;

namespace {

    struct TabletLt
    {
        warp::IntervalPointOrder<warp::less> lt;

        bool operator()(TabletPtr const & a, TabletPtr const & b)
        {
            return lt(a->getRows().getUpperBound(),
                      b->getRows().getUpperBound());
        }

        bool operator()(TabletPtr const & a, IntervalPoint<string> const & b)
        {
            return lt(a->getRows().getUpperBound(), b);
        }

        bool operator()(IntervalPoint<string> const & a, TabletPtr const & b)
        {
            return lt(a, b->getRows().getUpperBound());
        }

        bool operator()(TabletPtr const & a, strref_t b)
        {
            return lt(a->getRows().getUpperBound(), b);
        }

        bool operator()(strref_t a, TabletPtr const & b)
        {
            return lt(a, b->getRows().getUpperBound());
        }
    };

}

//----------------------------------------------------------------------------
// SuperTablet
//----------------------------------------------------------------------------
TabletPtr const & SuperTablet::getTablet(strref_t row) const
{
    // Find tablet such that its upper bound contains the row
    TabletLt tlt;
    vector<TabletPtr>::const_iterator i = std::upper_bound(
        tablets.begin(), tablets.end(), row, tlt);

    // If the tablet lower bound also contains the row, we have a
    // match
    if(i != tablets.end() && tlt.lt((*i)->getRows().getLowerBound(), row))
        return *i;
    else
        raise<ValueError>("row not on this server: %s", reprString(row));
}

SuperTablet::SuperTablet(std::string const & name,
                         MetaConfigManagerPtr const & configMgr,
                         SharedLoggerSyncPtr const & syncLogger,
                         SharedCompactorPtr const & compactor)
{
    TabletName minRow(name, IntervalPoint<string>("", PT_INCLUSIVE_UPPER_BOUND));
    TabletName maxRow(name, IntervalPoint<string>("", PT_INFINITE_UPPER_BOUND));

    // Scan all tablet rows for this table in the META table.
    // Correct inconsistent rows that may have been the result of
    // a mid-split crash.  Load the ones that are assigned to this
    // server.
    TablePtr metaTable = configMgr->getMetaTable();
    std::string const & serverName = configMgr->getServerName();
        
    CellStreamPtr metaScan = metaTable->scan(
        str(format("%s <= row <= %s and column = 'config'")
            % minRow.getEncoded() % maxRow.getEncoded())
        );

    IntervalPoint<string> lowerBound("", PT_INFINITE_LOWER_BOUND);
    IntervalPoint<string> prevLowerBound(lowerBound);

    bool changedMeta = false;
    bool loadedPrev = false;
    Cell prev(0,0);
    Cell x;
    while(metaScan->get(x))
    {
        TabletConfig cfg = configMgr->getConfigFromCell(x);
        Interval<string> const & cfgRows = cfg.getTabletRows();
        if(cfgRows.getLowerBound() < lowerBound)
        {
            // We have an overlap -- this cell overlaps with the
            // previous cell.
            log("Detected overlap: prev=%s cur=%s", prev, x);

            // First, make sure this is actually from a partial split.
            if(cfgRows.getLowerBound() != prevLowerBound)
            {
                raise<RuntimeError>("uncorrectable overlap in META table: "
                                    "prev=%s cur=%s", prev, x);
            }

            // To repair the situation, we should delete the previous
            // cell.
            assert(prev);
            metaTable->erase(prev.getRow(), prev.getColumn(),
                             prev.getTimestamp());
            changedMeta = true;

            // Also erase previous tablet if we loaded it
            if(loadedPrev)
                tablets.pop_back();
        }
        else if(lowerBound < cfgRows.getLowerBound())
        {
            // We have a gap -- this cell is not adjacent to the
            // previous cell.
            log("Detected gap: prev=%s cur=%s", prev, x);

            // To repair this situation, we should expand this cell to
            // fill the gap.
            cfg = TabletConfig(
                Interval<string>(lowerBound, cfgRows.getUpperBound()),
                cfg.getTableUris(),
                cfg.getServer()
                );
            metaTable->set(x.getRow(), x.getColumn(), x.getTimestamp(),
                           configMgr->getConfigCellValue(cfg));
            changedMeta = true;
        }

        if(cfg.getServer() == serverName)
        {
            // We found a tablet assigned to us
            TabletPtr p(
                new Tablet(
                    x.getRow().toString(),
                    configMgr,
                    syncLogger,
                    compactor,
                    cfg
                    )
                );
            tablets.push_back(p);
            loadedPrev = true;
        }
        else
            loadedPrev = false;

        // Update lower bound
        prevLowerBound = lowerBound;
        lowerBound = cfgRows.getUpperBound().getAdjacentComplement();
        prev = x;
    }

    if(changedMeta)
        metaTable->sync();
}

SuperTablet::~SuperTablet()
{
}

void SuperTablet::set(strref_t row, strref_t column, int64_t timestamp,
                      strref_t value)
{
    getTablet(row)->set(row,column,timestamp,value);
}

void SuperTablet::erase(strref_t row, strref_t column, int64_t timestamp)
{
    getTablet(row)->erase(row,column,timestamp);
}

void SuperTablet::insert(Cell const & x)
{
    getTablet(x.getRow())->insert(x);
}

CellStreamPtr SuperTablet::scan() const
{
    return scan(ScanPredicate());
}

CellStreamPtr SuperTablet::scan(ScanPredicate const & pred) const
{
    // Figure out row range in predicate
    Interval<string> rows;
    if(pred.getRowPredicate())
        rows = pred.getRowPredicate()->getHull();
    else
        rows.setInfinite();

    // Map row range to a single tablet
    TabletLt tlt;
    vector<TabletPtr>::const_iterator i = std::upper_bound(
        tablets.begin(), tablets.end(), rows.getLowerBound(), tlt);
    if(i != tablets.end() && (*i)->getRows().contains(rows))
        return (*i)->scan(pred);
    else
        raise<ValueError>("predicate does not map to a single tablet");
}

void SuperTablet::sync()
{
    for(vector<TabletPtr>::const_iterator i = tablets.begin();
        i != tablets.end(); ++i)
    {
        (*i)->sync();
    }
}
