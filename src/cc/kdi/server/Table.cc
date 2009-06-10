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
#include <kdi/server/TabletConfig.h>
#include <kdi/server/Serializer.h>
#include <kdi/server/Compactor.h>
#include <kdi/server/FragmentWriterFactory.h>
#include <kdi/server/FragmentLoader.h>
#include <kdi/server/errors.h>
#include <kdi/scan_predicate.h>
#include <warp/fs.h>
#include <warp/file.h>
#include <warp/log.h>
#include <warp/string_interval.h>
#include <ex/exception.h>

using namespace kdi;
using namespace kdi::server;
using namespace ex;

using warp::log;

namespace {

    struct FirstEq
    {
        template <class A, class B, class C>
        bool operator()(A const & a, std::pair<B,C> const & b) const
        {
            return a == b.first;
        }

        template <class A, class B, class C>
        bool operator()(std::pair<A,B> const & a, B const & b) const
        {
            return a.first == b;
        }
    };
    
}

//----------------------------------------------------------------------------
// Table::TabletLt
//----------------------------------------------------------------------------
class Table::TabletLt
{
    warp::IntervalPointOrder<warp::less> lt;

    inline static warp::IntervalPoint<std::string> const &
    get(Tablet const * x) { return x->getLastRow(); }

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
    if(!lt((*i)->getFirstRow(), row))
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
    if(lt(last, (*i)->getLastRow()))
        return 0;

    return *i;
}

Tablet * Table::getTablet(warp::IntervalPoint<std::string> const & last) const
{
    Tablet * t = findTablet(last);
    if(!t)
        throw TabletNotLoadedError();

    return t;
}

void Table::verifyTabletsLoaded(std::vector<warp::StringRange> const & rows,
                                std::vector<Tablet *> & loadingTablets) const
{
    for(std::vector<warp::StringRange>::const_iterator i = rows.begin();
        i != rows.end(); ++i)
    {
        Tablet * tablet = findContainingTablet(*i);
        if(!tablet)
            throw TabletNotLoadedError();

        if(tablet->getState() <= TABLET_CONFIG_SAVING)
        {
            if(loadingTablets.empty() || loadingTablets.back() != tablet)
                loadingTablets.push_back(tablet);
        }
        else if(tablet->getState() > TABLET_ACTIVE)
            throw TabletNotLoadedError();
    }
}

void Table::verifyColumnFamilies(std::vector<std::string> const & columns) const
{
    for(std::vector<std::string>::const_iterator i = columns.begin();
        i != columns.end(); ++i)
    {
        if(!groupIndex.contains(*i))
            throw UnknownColumnFamilyError();
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

Tablet * Table::createTablet(warp::IntervalPoint<std::string> const & lastRow)
{
    assert(!findTablet(lastRow));

    tablet_vec::iterator i = tablets.insert(
        std::lower_bound(
            tablets.begin(),
            tablets.end(),
            lastRow,
            TabletLt()),
        0);
    
    *i = new Tablet(lastRow);
    (*i)->applySchema(schema);

    return *i;
}

size_t Table::getMaxDiskChainLength(int groupIndex) const
{
    size_t maxLen = 0;
    for(tablet_vec::const_iterator i = tablets.begin();
        i != tablets.end(); ++i)
    {
        size_t len = (*i)->getChainLength(groupIndex);
        if(len > maxLen)
            maxLen = len;
    }
    return maxLen;
}

void Table::getCompactionSet(int groupIndex, RangeFragmentMap & compactionSet) const
{
    frag_vec frags;
    for(tablet_vec::const_iterator i = tablets.begin();
        i != tablets.end(); ++i)
    {
        frags.clear();
        (*i)->getFragments(frags, groupIndex);

        if(frags.size() > 1)
            compactionSet.addFragments((*i)->getRows(), frags);
    }
}

void Table::replaceMemFragments(
    std::vector<FragmentCPtr> const & oldFragments,
    FragmentCPtr const & newFragment,
    int groupIndex,
    std::vector<std::string> const & rowCoverage,
    std::vector<Tablet *> & updatedTablets)
{
    log("Table %s: replace %d mem frag(s) with %s",
        schema.tableName, oldFragments.size(),
        newFragment ? newFragment->getFilename() : "(NOTHING)");

    // Make sure the oldFragment list is at the head of the memory
    // chain
    memfrag_vec & memFrags = groupMemFrags[groupIndex];
    if(memFrags.size() < oldFragments.size())
        return;
    if(!std::equal(oldFragments.begin(), oldFragments.end(),
                   memFrags.begin(), FirstEq()))
        return;

    // Remove the old memory fragments
    memFrags.erase(memFrags.begin(),
                   memFrags.begin() + oldFragments.size());

    // Add the new fragment to all covered tablets
    size_t origSz = updatedTablets.size();
    warp::IntervalPointOrder<warp::less> lt;
    std::vector<std::string>::const_iterator ri = rowCoverage.begin();
    while(ri != rowCoverage.end())
    {
        tablet_vec::iterator ti = std::lower_bound(
            tablets.begin(), tablets.end(), *ri, TabletLt());

        if(ti == tablets.end())
        {
            // We have no tablets that could contain this or any later
            // rows.
            break;
        }
        else if(lt((*ti)->getFirstRow(), *ri))
        {
            // Track the changed tablet
            updatedTablets.push_back(*ti);

            // Found tablet containing row.  Add the new fragment to
            // it.
            (*ti)->addFragment(newFragment, groupIndex);

            // Split tablet if necessary
            Tablet * newTablet = (*ti)->maybeSplit();
            if(newTablet)
            {
                // We made a new Tablet.  The split tablet should be
                // the lower half of the original Tablet, so insert it
                // before the original Tablet.
                ti = tablets.insert(ti, newTablet);

                // Track the new Tablet as well.
                updatedTablets.push_back(*ti);

                // Move iterator back to original Tablet.
                ++ti;
            }
            
            // Move row pointer past end of this tablet
            ri = std::lower_bound(ri, rowCoverage.end(),
                                  (*ti)->getLastRow(),
                                  lt);
        }
        else
        {
            // The found tablet does not contain this row.  Move the
            // row pointer past the beginning of the tablet.
            ri = std::lower_bound(ri, rowCoverage.end(),
                                  (*ti)->getFirstRow(),
                                  lt);
        }
    }

    log("Table %s: updated %d tablet(s)",
        schema.tableName, updatedTablets.size() - origSz);
}

void Table::replaceDiskFragments(
    std::vector<FragmentCPtr> const & oldFragments,
    FragmentCPtr const & newFragment,
    int groupIndex,
    warp::Interval<std::string> const & rowRange,
    std::vector<Tablet *> & updatedTablets)
{
    log("Table %s: replace %d disk frag(s) with %s",
        schema.tableName, oldFragments.size(),
        newFragment ? newFragment->getFilename() : "(NOTHING)");

    size_t origSz = updatedTablets.size();

    tablet_vec::const_iterator end = std::upper_bound(
        tablets.begin(), tablets.end(),
        rowRange.getUpperBound(),
        TabletLt());
    
    tablet_vec::const_iterator begin = end;
    while(begin != tablets.begin() &&
          rowRange.contains((*(begin-1))->getRows()))
        --begin;

    for(tablet_vec::const_iterator i = begin; i != end; ++i)
    {
        updatedTablets.push_back(*i);
        (*i)->replaceFragments(oldFragments, newFragment, groupIndex);
    }

    log("Table %s: updated %d tablet(s)",
        schema.tableName, updatedTablets.size() - origSz);
}

Table::Table(std::string const & tableName) :
    schemaVersion(0)
{
    schema.initDefault(tableName);
}

Table::~Table()
{
    std::for_each(tablets.begin(), tablets.end(), warp::delete_ptr());
}

void Table::getFirstFragmentChain(ScanPredicate const & pred,
                                  std::vector<FragmentCPtr> & chain,
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
    if(lt(first, (*i)->getFirstRow()))
        throw TabletNotLoadedError();

    // Find mapping from predicate to locality groups
    std::vector<int> predGroups;
    getPredicateGroups(pred, predGroups);

    chain.clear();
    for(std::vector<int>::const_iterator gi = predGroups.begin();
        gi != predGroups.end(); ++gi)
    {
        (*i)->getFragments(chain, *gi);
        getMemFragments(*gi, chain);
    }
    rows = (*i)->getRows();
}

void Table::getPredicateGroups(ScanPredicate const & pred, std::vector<int> & out) const
{
    out.clear();

    if(!pred.getColumnPredicate())
    {
        // Need all groups
        out.resize(schema.groups.size());
        for(size_t i = 0; i < out.size(); ++i)
            out[i] = i;

        return;
    }
    
    std::vector<warp::StringRange> fams;
    if(pred.getColumnFamilies(fams))
    {
        // Predicate maps to discrete column families.

        if(fams.empty())
            return;

        out.reserve(fams.size());
        for(size_t i = 0; i < fams.size(); ++i)
            out.push_back(groupIndex.get(fams[i]));
    }
    else
    {
        // Predicate spans column families.
        ScanPredicate::StringSetCPtr cols = pred.getColumnPredicate();

        for(size_t i = 0; i < schema.groups.size(); ++i)
        {
            TableSchema::Group const & g = schema.groups[i];
            for(std::vector<std::string>::const_iterator j = g.families.begin();
                j != g.families.end(); ++j)
            {
                if(cols->overlaps(warp::makePrefixInterval(*j + ":")))
                {
                    out.push_back(i);
                    continue;
                }
            }
        }
    }

    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
}

void Table::addMemoryFragment(FragmentCPtr const & frag, int64_t txn)
{
    typedef std::vector<std::string> str_vec;
    typedef std::tr1::unordered_set<int> group_set;

    // Get all the column families in the fragment
    str_vec families;
    frag->getColumnFamilies(families);

    // Figure out the set of groups covered by the fragment
    group_set fragGroups;
    for(str_vec::const_iterator i = families.begin();
        i != families.end(); ++i)
    {
        int idx = groupIndex.get(*i, -1);
        if(idx >= 0)
            fragGroups.insert(idx);
    }

    // Add the memory fragment to all affected groups
    for(group_set::const_iterator i = fragGroups.begin();
        i != fragGroups.end(); ++i)
    {
        groupMemFrags[*i].push_back(
            memfrag(
                frag->getRestricted(schema.groups[*i].families),
                txn));
    }
}

void Table::addLoadedFragments(warp::Interval<std::string> const & rows,
                               std::vector<FragmentCPtr> const & frags)
{
    Tablet * tablet = findTablet(rows.getUpperBound());
    assert(tablet);
    assert(rows.getLowerBound() == tablet->getFirstRow());

    typedef std::vector<std::string> str_vec;
    typedef std::tr1::unordered_set<int> group_set;
    str_vec families;
    group_set fragGroups;

    for(std::vector<FragmentCPtr>::const_iterator fi = frags.begin();
        fi != frags.end(); ++fi)
    {
        // Get all the column families in the fragment
        families.clear();
        (*fi)->getColumnFamilies(families);

        // Figure out the set of groups covered by the fragment
        fragGroups.clear();
        for(str_vec::const_iterator i = families.begin();
            i != families.end(); ++i)
        {
            int idx = groupIndex.get(*i, -1);
            if(idx >= 0)
                fragGroups.insert(idx);
        }

        // Add the fragment to all affected groups
        for(group_set::const_iterator i = fragGroups.begin();
            i != fragGroups.end(); ++i)
        {
            tablet->addFragment(
                (*fi)->getRestricted(schema.groups[*i].families),
                *i);
        }
    }
}

void Table::applySchema(TableSchema const & s)
{
    typedef std::vector<std::string> str_vec;

    // Bump schema version
    ++schemaVersion;

    // Copy new schema
    schema = s;

    // Rebuild group index
    groupIndex.clear();
    for(size_t i = 0; i < schema.groups.size(); ++i)
    {
        typedef std::vector<std::string> str_vec;
        str_vec const & families = schema.groups[i].families;

        for(str_vec::const_iterator j = families.begin();
            j != families.end(); ++j)
        {
            groupIndex.set(*j, i);
        }
    }

    // Rebuild memory fragment chains
    memfrag_vec oldMemFrags;
    for(memfragvec_vec::const_iterator i = groupMemFrags.begin();
        i != groupMemFrags.end(); ++i)
    {
        // XXX instead of tightening the restrictions, we could
        // topo-merge the fragment chains and then partition them.
        // It's probably not that critical since these are just memory
        // fragments and should be flushed soon.
        oldMemFrags.insert(oldMemFrags.end(), i->begin(), i->end());
    }
    groupMemFrags.clear();
    groupMemFrags.resize(schema.groups.size());
    for(memfrag_vec::const_iterator i = oldMemFrags.begin();
        i != oldMemFrags.end(); ++i)
    {
        addMemoryFragment(i->first, i->second);
    }
    oldMemFrags.clear();

    // Apply new locality groups to all Tablets
    for(tablet_vec::iterator i = tablets.begin();
        i != tablets.end(); ++i)
    {
        (*i)->applySchema(schema);
    }
}

void Table::getMemFragments(int groupIndex, std::vector<FragmentCPtr> & out) const
{
    memfrag_vec const & f = groupMemFrags[groupIndex];
    for(memfrag_vec::const_iterator i = f.begin(); i != f.end(); ++i)
        out.push_back(i->first);
}

size_t Table::getMemSize(int groupIndex) const
{
    size_t sum = 0;
    memfrag_vec const & f = groupMemFrags[groupIndex];
    for(memfrag_vec::const_iterator i = f.begin(); i != f.end(); ++i)
    {
        sum += i->first->getDataSize();
    }
    return sum;
}

int64_t Table::getEarliestMemCommit(int groupIndex) const
{
    memfrag_vec const & f = groupMemFrags[groupIndex];
    if(f.empty())
        return rowCommits.getMaxCommit();
    else
        return f.front().second;
}

int64_t Table::getEarliestMemCommit() const
{
    int64_t minCommit = rowCommits.getMaxCommit();
    for(memfragvec_vec::const_iterator i = groupMemFrags.begin();
        i != groupMemFrags.end(); ++i)
    {
        if(i->empty())
            continue;

        if(i->front().second < minCommit)
            minCommit = i->front().second;
    }
    return minCommit;
}
