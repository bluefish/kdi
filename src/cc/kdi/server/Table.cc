//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-10
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

#include <kdi/server/Table.h>
#include <kdi/server/Tablet.h>
#include <kdi/server/errors.h>
#include <kdi/scan_predicate.h>
#include <ex/exception.h>

using namespace kdi;
using namespace kdi::server;
using namespace ex;

//----------------------------------------------------------------------------
// Table::TabletLt
//----------------------------------------------------------------------------
class Table::TabletLt
{
    warp::IntervalPointOrder<warp::less> lt;

    inline static warp::IntervalPoint<std::string> const &
    get(Tablet const * x) { return x->getMaxRow(); }

    inline static warp::IntervalPoint<std::string> const &
    get(warp::IntervalPoint<std::string> const & x) { return x; }

    inline static strref_t get(strref_t x) { return x; }

public:
    template <class A, class B>
    bool operator()(A const & a, B const & b) const
    {
        return lt(get(a), get(b));
    }
};

//----------------------------------------------------------------------------
// Table
//----------------------------------------------------------------------------
Tablet * Table::findContainingTablet(strref_t row) const
{
    tablet_vec::const_iterator i = std::lower_bound(
        tablets.begin(), tablets.end(), row, TabletLt());
    if(i == tablets.end())
        return 0;
    
    warp::IntervalPointOrder<warp::less> lt;
    if(!lt((*i)->getMinRow(), row))
        return 0;

    return *i;
}

Tablet * Table::findTablet(warp::IntervalPoint<std::string> const & last) const
{
    tablet_vec::const_iterator i = std::lower_bound(
        tablets.begin(), tablets.end(), last, TabletLt());
    if(i == tablets.end())
        return 0;
    
    warp::IntervalPointOrder<warp::less> lt;
    if(lt(last, (*i)->getMaxRow()))
        return 0;

    return *i;
}

bool Table::isTabletLoaded(strref_t tabletName) const
{
    std::string table;
    warp::IntervalPoint<std::string> last;
    decodeTabletName(tabletName, table, last);

    assert(table == schema.name);
    return findTablet(last) != 0;
}

void Table::verifyTabletsLoaded(std::vector<warp::StringRange> const & rows) const
{
    for(std::vector<warp::StringRange>::const_iterator i = rows.begin();
        i != rows.end(); ++i)
    {
        if(!findContainingTablet(*i))
            throw TabletNotLoadedError();
    }
}
    
void Table::verifyCommitApplies(std::vector<warp::StringRange> const & rows,
                                int64_t maxTxn) const
{
    if(maxTxn >= rowCommits.getMaxCommit())
        return;
    
    if(maxTxn < rowCommits.getMinCommit())
        throw MutationConflictError();

    for(std::vector<warp::StringRange>::const_iterator i = rows.begin();
        i != rows.end(); ++i)
    {
        if(maxTxn < rowCommits.getCommit(*i))
            throw MutationConflictError();
    }
}

void Table::updateRowCommits(std::vector<warp::StringRange> const & rows,
                             int64_t commitTxn)
{
    for(std::vector<warp::StringRange>::const_iterator i = rows.begin();
        i != rows.end(); ++i)
    {
        rowCommits.setCommit(*i, commitTxn);
    }
}

Tablet * Table::createTablet(warp::Interval<std::string> const & rows)
{
    tablets.push_back(new Tablet(rows));
    return tablets.back();
}

Table::~Table()
{
    std::for_each(tablets.begin(), tablets.end(), warp::delete_ptr());
}

void Table::getFirstFragmentChain(ScanPredicate const & pred,
                                  std::vector<Fragment const *> & chain,
                                  warp::Interval<std::string> & rows) const
{
    warp::IntervalPoint<std::string> first(
        std::string(), warp::PT_INCLUSIVE_LOWER_BOUND);
    if(ScanPredicate::StringSetCPtr x = pred.getRowPredicate())
    {
        if(x->isEmpty())
            raise<ValueError>("empty row predicate");
        first = *x->begin();
    }
    
    tablet_vec::const_iterator i = std::lower_bound(
        tablets.begin(), tablets.end(), first, TabletLt());
    if(i == tablets.end())
        throw TabletNotLoadedError();
    
    warp::IntervalPointOrder<warp::less> lt;
    if(lt(first, (*i)->getMinRow()))
        throw TabletNotLoadedError();

    (*i)->getFragments(chain);
    memFrags.getFragments(chain);
    rows = (*i)->getRows();
}
