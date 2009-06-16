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
#include <warp/string_median.h>
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
    // Auto export all Fragments
    for(fragvec_vec::const_iterator i = fragGroups.begin();
        i != fragGroups.end(); ++i)
    {
        for(frag_vec::const_iterator j = i->begin();
            j != i->end(); ++j)
        {
            (*j)->exportFragment();
        }
    }
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

    inline
    warp::IntervalPoint<std::string> bisectInterval(
        warp::IntervalPoint<std::string> const & lo,
        warp::IntervalPoint<std::string> const & hi)
    {
        using namespace warp;
        using std::string;

        if(lo.isFinite())
        {
            if(hi.isFinite())
                return IntervalPoint<string>(
                    string_median(lo.getValue(), hi.getValue()),
                    PT_INCLUSIVE_UPPER_BOUND);
            else
            {
                string const & loString = lo.getValue();
                string hiString;
                while(hiString.size() <= loString.size())
                    hiString += '\xff';

                return IntervalPoint<string>(
                    string_median(loString, hiString),
                    PT_INCLUSIVE_UPPER_BOUND);
            }
        }
        else
        {
            if(hi.isFinite())
                return IntervalPoint<string>(
                    string_median("", hi.getValue()),
                    PT_INCLUSIVE_UPPER_BOUND);
            else
                return IntervalPoint<string>(
                    "\x7f", PT_INCLUSIVE_UPPER_BOUND);
        }
    }

}

#include <sstream>
#include <warp/repr.h>

namespace {

    template <class T>
    class ReprWrapper
    {
        T const & valueRef;

    public:
        explicit ReprWrapper(T const & t) : valueRef(t) {}
        T const & operator*() const { return valueRef; }
        T const * operator->() const { return &valueRef; }
    };

    template <class T>
    ReprWrapper<T> repr(T const & t)
    {
        return ReprWrapper<T>(t);
    }

    template <class T>
    std::string reprStr(T const & t)
    {
        std::ostringstream oss;
        oss << repr(t);
        return oss.str();
    }

    std::ostream & operator<<(std::ostream & o, ReprWrapper<std::string> const & x)
    {
        return o << '"' << warp::ReprEscape(*x) << '"';
    }

    template <class T>
    std::ostream & operator<<(std::ostream & o, ReprWrapper<warp::IntervalPoint<T> > const & x)
    {
        switch(x->getType())
        {
            case warp::PT_INFINITE_LOWER_BOUND:  return o << "<<";
            case warp::PT_EXCLUSIVE_UPPER_BOUND: return o << repr(x->getValue()) << ')';
            case warp::PT_INCLUSIVE_LOWER_BOUND: return o << '[' << repr(x->getValue());
            case warp::PT_POINT:                 return o << repr(x->getValue());
            case warp::PT_INCLUSIVE_UPPER_BOUND: return o << repr(x->getValue()) << ']';
            case warp::PT_EXCLUSIVE_LOWER_BOUND: return o << '(' << repr(x->getValue());
            case warp::PT_INFINITE_UPPER_BOUND:  return o << ">>";
            default:                             return o << "<?>";
        }
    }

    template <class T>
    std::ostream & operator<<(std::ostream & o, ReprWrapper<warp::Interval<T> > const & x)
    {
        return o << repr(x->getLowerBound()) << ' ' << repr(x->getUpperBound());
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

    log("Splitting tablet: %s, rows=%s", getTabletName(), repr(getRows()));

    typedef warp::Interval<std::string> ival_t;
    typedef warp::IntervalPoint<std::string> ival_pt_t;

    ival_pt_t lo = getFirstRow();
    ival_pt_t hi = getLastRow();

    struct split
    {
        ival_pt_t mid;
        size_t loSz;
        size_t hiSz;
        size_t diff;
    };

    split best;
    best.diff = std::numeric_limits<size_t>::max();

    size_t const MAX_ITERATIONS = 100;
    size_t iter = 0;
    try {
        while(++iter <= MAX_ITERATIONS)
        {
            split s;
            s.mid = bisectInterval(lo, hi);

            //log("Iter %d: bisect %s %s : %s", iter, repr(lo), repr(hi), repr(s.mid));

            s.loSz = getPartialDataSize(ival_t(getFirstRow(), s.mid));
            s.hiSz = getPartialDataSize(ival_t(s.mid.getAdjacentComplement(),
                                               getLastRow()));
            if(s.loSz < s.hiSz)
            {
                lo = s.mid.getAdjacentComplement();
                s.diff = s.hiSz - s.loSz;
            }
            else
            {
                hi = s.mid;
                s.diff = s.loSz - s.hiSz;
            }

            //log("Iter %d: mid = %s, loSz = %d (%.2f%%), hiSz = %d (%.2f%%), "
            //    "diff = %d (%.3f%%)",
            //    iter, repr(s.mid),
            //    s.loSz, s.loSz*100.0/totalSz,
            //    s.hiSz, s.hiSz*100.0/totalSz,
            //    s.diff, s.diff*100.0/totalSz);

            if(s.diff < best.diff)
            {
                best = s;
                if(best.diff < totalSz * 0.002)
                    break;
            }
        }
    }
    catch(warp::no_median_error const &) {
        log("Split: no median");
    }

    log("Best split after %d iteration(s): lo=%s, mid=%s, hi=%s, loSz=%d (%.2f%%), hiSz=%d (%.2f%%), diff=%d (%.3f%%)",
        iter, repr(getFirstRow()), repr(best.mid), repr(getLastRow()),
        best.loSz, best.loSz*100.0/totalSz,
        best.hiSz, best.hiSz*100.0/totalSz,
        best.diff, best.diff*100.0/totalSz);

    if(best.diff > totalSz * 0.30 || best.loSz > totalSz * 0.60)
    {
        log("Couldn't find a good split");
        return 0;
    }

    Tablet * newTablet = new Tablet(best.mid);
    newTablet->firstRow = getFirstRow();
    newTablet->state = TABLET_ACTIVE;
    newTablet->fragGroups = this->fragGroups;

    this->firstRow = best.mid.getAdjacentComplement();

    return newTablet;
}
