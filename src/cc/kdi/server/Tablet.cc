//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-17
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

#include <kdi/server/Tablet.h>
#include <kdi/server/TableSchema.h>
#include <kdi/server/TabletConfig.h>
#include <warp/Callback.h>
#include <warp/log.h>
#include <warp/strutil.h>
#include <ex/exception.h>
#include <memory>
#include <limits>
#include <string>
#include <algorithm>

using namespace kdi::server;
using warp::log;

namespace {

    class CbCaller
        : public warp::Runnable
    {
    public:
        void run() throw()
        {
            for(std::vector<warp::Callback *>::const_iterator i = cbs.begin();
                i != cbs.end(); ++i)
            {
                (*i)->done();
            }
            delete this;
        }

        void addCallback(warp::Callback * cb)
        {
            cbs.push_back(cb);
        }

    private:
        std::vector<warp::Callback *> cbs;
    };

}

//----------------------------------------------------------------------------
// Tablet
//----------------------------------------------------------------------------
Tablet::Tablet(warp::IntervalPoint<std::string> const & lastRow) :
    lastRow(lastRow),
    firstRow(std::string(), warp::PT_INFINITE_LOWER_BOUND),
    state(TABLET_CONFIG_LOADING)
{
}

Tablet::~Tablet()
{
}

std::string Tablet::getTabletName() const
{
    if(lastRow.isFinite())
        return warp::reprString(lastRow.getValue());
    else
        return "END";
}

void Tablet::applySchema(TableSchema const & schema)
{
    // xxx lazy...
    using namespace ex;
    if(schema.groups.size() != 1)
        raise<RuntimeError>("only support single locality group");

    fragGroups.resize(schema.groups.size());
}

void Tablet::getConfigFragments(TabletConfig & cfg) const
{
    size_t n = 0;
    for(fragvec_vec::const_iterator i = fragGroups.begin();
        i != fragGroups.end(); ++i)
    {
        n += i->size();
    }

    if(loadingConfig)
        n += loadingConfig->fragments.size();

    cfg.fragments.clear();
    cfg.fragments.resize(n);

    std::vector<TabletConfig::Fragment>::iterator oi = cfg.fragments.begin();

    if(loadingConfig)
    {
        oi = std::copy(loadingConfig->fragments.begin(),
                       loadingConfig->fragments.end(),
                       oi);
    }

    for(fragvec_vec::const_iterator i = fragGroups.begin();
        i != fragGroups.end(); ++i)
    {
        for(frag_vec::const_iterator j = i->begin();
            j != i->end(); ++j, ++oi)
        {
            oi->filename = (*j)->getFilename();
            (*j)->getColumnFamilies(oi->families);
        }
    }
}

void Tablet::onConfigLoaded(TabletConfigCPtr const & config)
{
    assert(state == TABLET_CONFIG_LOADING);
    assert(config->rows.getUpperBound() == lastRow);
    
    firstRow = config->rows.getLowerBound();
    if(!config->fragments.empty())
        loadingConfig = config;
}

void Tablet::onFragmentsLoaded(
    TableSchema const & schema,
    std::vector<FragmentCPtr> const & fragments)
{
    assert(state == TABLET_FRAGMENTS_LOADING);
    assert(loadingConfig);
    assert(schema.groups.size() == fragGroups.size());

    // xxx lazy...
    using namespace ex;
    if(schema.groups.size() != 1)
        raise<RuntimeError>("only support single locality group");

    fragGroups[0].insert(
        fragGroups[0].begin(),
        fragments.begin(),
        fragments.end());

    loadingConfig.reset();
}

void Tablet::deferUntilState(warp::Callback * cb, TabletState state)
{
    cbHeap.push(state_cb(state, cb));
}

warp::Runnable * Tablet::setState(TabletState state)
{
    std::auto_ptr<CbCaller> r;

    this->state = state;

    while(!cbHeap.empty())
    {
        if(cbHeap.top().first > state)
            break;

        if(!r.get())
            r.reset(new CbCaller);

        r->addCallback(cbHeap.top().second);
        cbHeap.pop();
    }

    return r.release();
}

size_t Tablet::getPartialDataSize(
    warp::Interval<std::string> const & rows) const
{
    size_t totalSz = 0;
    
    for(fragvec_vec::const_iterator i = fragGroups.begin();
        i != fragGroups.end(); ++i)
    {
        for(frag_vec::const_iterator j = i->begin();
            j != i->end(); ++j)
        {
            totalSz += (*j)->getPartialDataSize(rows);
        }
    }

    return totalSz;
}

namespace {

    inline void
    midstr(char const * a, char const * aEnd,
           char const * b, char const * bEnd,
           std::string & out)
    {
        for(;;)
        {
            int av = a != aEnd ? uint8_t(*a++) : 0;
            int bv = b != bEnd ? uint8_t(*b++) : 256;
            int mv = (av + bv) / 2;
            out += char(mv);
            if(mv != av)
                break;
        }
    }

    inline
    warp::IntervalPoint<std::string> bisectInterval(
        warp::IntervalPoint<std::string> const & lo,
        warp::IntervalPoint<std::string> const & hi)
    {
        std::string const & a = lo.getValue();
        std::string const & b = hi.getValue();

        size_t offset = 0;
        if(lo.isFinite() && hi.isFinite())
        {
            // Offset = first mismatch position
            if(a.size() < b.size())
                offset = std::mismatch(a.begin(), a.end(), b.begin()).first
                    - a.begin();
            else
                offset = std::mismatch(b.begin(), b.end(), a.begin()).first
                    - b.begin();
        }

        std::string m = a.substr(0,offset);
        midstr(a.c_str() + offset, a.c_str() + a.size(),
               b.c_str() + offset, b.c_str() + b.size(),
               m);

        return warp::IntervalPoint<std::string>(
            m, warp::PT_INCLUSIVE_UPPER_BOUND);
    }

}

Tablet * Tablet::maybeSplit()
{
    if(state != TABLET_ACTIVE)
        return 0;

    size_t totalSz = getPartialDataSize(getRows());

    size_t const TABLET_SPLIT_THRESHOLD = size_t(200) << 20;
    if(totalSz < TABLET_SPLIT_THRESHOLD)
        return 0;

    log("Splitting tablet: %s", getTabletName());

    typedef warp::Interval<std::string> ival_t;
    typedef warp::IntervalPoint<std::string> ival_pt_t;

    ival_pt_t lo = getFirstRow();
    ival_pt_t hi = getLastRow();

    ival_pt_t bestSplit;
    size_t minDiff = std::numeric_limits<size_t>::max();

    size_t const MAX_ITERATIONS = 100;
    for(size_t i = 0; i < MAX_ITERATIONS; ++i)
    {
        ival_pt_t mid = bisectInterval(lo, hi);

        size_t loSz = getPartialDataSize(ival_t(getFirstRow(), mid));
        size_t hiSz = getPartialDataSize(ival_t(mid.getAdjacentComplement(),
                                                getLastRow()));

        size_t diff;
        if(loSz < hiSz)
        {
            lo = mid.getAdjacentComplement();
            diff = hiSz - loSz;
        }
        else
        {
            hi = mid;
            diff = loSz - hiSz;
        }

        log("Iter %d: mid = %s, loSz = %d (%.2f%%), hiSz = %d (%.2f%%), "
            "diff = %d (%.3f%%)",
            i, warp::reprString(mid.getValue()),
            loSz, loSz*100.0/totalSz,
            hiSz, hiSz*100.0/totalSz,
            diff, diff*100.0/totalSz);

        if(diff < minDiff)
        {
            minDiff = diff;
            bestSplit = mid;

            if(diff < totalSz * 0.002)
                break;
        }
    }

    if(minDiff > totalSz * 0.30)
    {
        log("Couldn't find a good split");
        return 0;
    }

    Tablet * newTablet = new Tablet(bestSplit);
    newTablet->firstRow = getFirstRow();
    newTablet->state = TABLET_ACTIVE;
    newTablet->fragGroups = this->fragGroups;

    this->firstRow = bestSplit.getAdjacentComplement();

    return newTablet;
}
