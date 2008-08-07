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
#include <kdi/tablet/SharedSplitter.h>
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
// SuperTablet::MutationInterlock
//----------------------------------------------------------------------------
class SuperTablet::MutationInterlock
{
    SuperTablet & x;
    
public:
    explicit MutationInterlock(SuperTablet & x) : x(x)
    {
        lock_t lock(x.mutex);
        while(x.mutationsBlocked)
            x.allowMutations.wait(lock);
        ++x.mutationsPending;
    }

    ~MutationInterlock()
    {
        lock_t lock(x.mutex);
        if(--x.mutationsPending == 0)
            x.allQuiet.notify_all();
    }
};


//----------------------------------------------------------------------------
// SuperTablet::MutationLull
//----------------------------------------------------------------------------
class SuperTablet::MutationLull
{
    SuperTablet & x;
    
public:
    explicit MutationLull(SuperTablet & x) : x(x)
    {
        lock_t lock(x.mutex);
        x.mutationsBlocked = true;
        while(x.mutationsPending)
            x.allQuiet.wait(lock);
    }

    ~MutationLull()
    {
        lock_t lock(x.mutex);
        x.mutationsBlocked = false;
        x.allowMutations.notify_all();
    }
};


//----------------------------------------------------------------------------
// SuperTablet
//----------------------------------------------------------------------------
SuperTablet::SuperTablet(std::string const & name,
                         MetaConfigManagerPtr const & configMgr,
                         SharedLoggerPtr const & logger,
                         SharedCompactorPtr const & compactor,
                         SharedSplitterPtr const & splitter) :
    splitter(splitter),
    mutationsBlocked(false),
    mutationsPending(0)
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
                *i,
                this
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
    MutationInterlock interlock(*this);
    getTablet(row)->set(row,column,timestamp,value);
}

void SuperTablet::erase(strref_t row, strref_t column, int64_t timestamp)
{
    MutationInterlock interlock(*this);
    getTablet(row)->erase(row,column,timestamp);
}

void SuperTablet::insert(Cell const & x)
{
    MutationInterlock interlock(*this);
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
    MutationInterlock interlock(*this);
    for(vector<TabletPtr>::const_iterator i = tablets.begin();
        i != tablets.end(); ++i)
    {
        log("SuperTablet::sync(): %s", (*i)->getPrettyName());

        (*i)->sync();
    }
}

void SuperTablet::requestSplit(Tablet * tablet)
{
    splitter->enqueueSplit(shared_from_this(), tablet);
}

void SuperTablet::performSplit(Tablet * tablet)
{
    // Stop mutations for this duration of this call
    MutationLull lull(*this);

    // Split the tablet at the given row
    TabletPtr lowTablet = tablet->splitTablet();
    
    // We may not get a new tablet if there was no place to split
    if(!lowTablet)
        return;

    // Insert the tablet into our map
    vector<TabletPtr>::iterator it = std::lower_bound(
        tablets.begin(), tablets.end(), lowTablet, TabletLt());
    tablets.insert(it, lowTablet);
}

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
