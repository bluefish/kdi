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
                         SharedLoggerPtr const & logger,
                         SharedCompactorPtr const & compactor)
{
    std::list<TabletConfig> cfgs = configMgr->loadTabletConfigs(name);
    
    if(cfgs.empty())
    {
        raise<RuntimeError>("Table has no tablets on this server: %s",
                            name);
    }

    for(std::list<TabletConfig>::const_iterator i = cfgs.begin();
        i != cfgs.end(); ++i)
    {
        TabletPtr p(
            new Tablet(
                name,
                configMgr,
                logger,
                compactor,
                *i
                )
            );
        tablets.push_back(p);
    }
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
        log("SuperTablet::sync(): %s", (*i)->getPrettyName());

        (*i)->sync();
    }
}
