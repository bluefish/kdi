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
#include <kdi/server/Serializer.h>
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

//----------------------------------------------------------------------------
// Table::TabletLt
//----------------------------------------------------------------------------
class Table::TabletLt
{
    warp::IntervalPointOrder<warp::less> lt;

    inline static warp::IntervalPoint<std::string> const &
    get(Tablet const * x) { return x->getRows().getUpperBound(); }

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
    if(!lt((*i)->getRows().getLowerBound(), row))
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
    if(lt(last, (*i)->getRows().getUpperBound()))
        return 0;

    return *i;
}

void Table::verifyTabletsLoaded(std::vector<warp::StringRange> const & rows) const
{
    for(std::vector<warp::StringRange>::const_iterator i = rows.begin();
        i != rows.end(); ++i)
    {
        Tablet * tablet = findContainingTablet(*i);
        if(!tablet)
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

Tablet * Table::createTablet(warp::Interval<std::string> const & rows)
{
    Tablet * t = new Tablet(rows);
    tablets.push_back(t);
    t->applySchema(schema);
    return t;
}

typedef boost::mutex::scoped_lock lock_t; 

void Table::serialize(Serializer & serialize, FragmentMaker const * fragMaker)
{
    frag_vec frags;
    unsigned groupIndex = 0;
    size_t curSchema = 0;
    TableSchema::Group group;

    {
        lock_t tableLock(tableMutex);

        // Choose the longest mem fragment chain and serialize it
        frag_vec & longestChain = groupMemFrags.front();
        for(fragvec_vec::iterator i = groupMemFrags.begin();
            i != groupMemFrags.end(); ++i)
        {
            if(i->size() >= longestChain.size())
            {
                longestChain = *i;
                groupIndex = i-groupMemFrags.begin();
            }
        }

        frags = longestChain;
        group = schema.groups[groupIndex];
        curSchema = applySchemaCtr;
    }

    // nothing to serialize?
    if(frags.empty()) return;

    // write out the new disk fragment and load it
    std::string fn = fragMaker->make(schema.tableName);
    serialize(frags, fn, group); 
    FragmentCPtr newFrag(new DiskFragment(fn));

    {
        lock_t tableLock(tableMutex);

        // did the schema change?
        if(curSchema != applySchemaCtr) return;

        // is the set of fragments to replace what we are expecting?
        frag_vec & memFrags = groupMemFrags[groupIndex];
        if(memFrags.size() < frags.size()) return;
        if(!std::equal(frags.begin(), frags.end(), memFrags.begin()))
            return;
        
        // find affected tablets and insert new fragments
        warp::StringRange row;
        while(serialize.getNextRow(row)) 
        {
            Tablet * t = findContainingTablet(row);
            t->addFragment(newFrag, groupIndex);

            warp::IntervalPoint<std::string> const & p = t->getRows().getUpperBound();
            if(p.isInfinite()) break; // at the last tablet
            row = p.getValue();
        }

        // remove the existing mem fragments
        memFrags.erase(memFrags.begin(), memFrags.begin()+frags.size());
    }
}

Table::Table() :
    applySchemaCtr(0)
{
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
    if(lt(first, (*i)->getRows().getLowerBound()))
        throw TabletNotLoadedError();

    // Find mapping from predicate to locality groups
    std::vector<int> predGroups;
    getPredicateGroups(pred, predGroups);

    chain.clear();
    for(std::vector<int>::const_iterator gi = predGroups.begin();
        gi != predGroups.end(); ++gi)
    {
        (*i)->getFragments(chain, *gi);
        chain.insert(chain.end(),
                     groupMemFrags[*gi].begin(),
                     groupMemFrags[*gi].end());
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
            for(std::vector<std::string>::const_iterator j = g.columns.begin();
                j != g.columns.end(); ++j)
            {
                if(cols->overlaps(warp::makePrefixInterval(*j)))
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

void Table::addMemoryFragment(FragmentCPtr const & frag)
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
            frag->getRestricted(schema.groups[*i].columns));
    }
}

void Table::addLoadedFragments(warp::Interval<std::string> const & rows,
                               std::vector<FragmentCPtr> const & frags)
{
    Tablet * tablet = findTablet(rows.getUpperBound());
    assert(tablet);
    assert(rows.getLowerBound() == tablet->getRows().getLowerBound());

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
                (*fi)->getRestricted(schema.groups[*i].columns),
                *i);
        }
    }
}

void Table::applySchema(TableSchema const & s)
{
    typedef std::vector<std::string> str_vec;
    typedef std::vector<FragmentCPtr> frag_vec;

    ++applySchemaCtr;

    // Copy new schema
    schema = s;

    // Rebuild group index
    groupIndex.clear();
    for(size_t i = 0; i < schema.groups.size(); ++i)
    {
        typedef std::vector<std::string> str_vec;
        str_vec const & cols = schema.groups[i].columns;

        for(str_vec::const_iterator j = cols.begin();
            j != cols.end(); ++j)
        {
            groupIndex.set(*j, i);
        }
    }

    // Rebuild memory fragment chains
    frag_vec oldMemFrags;
    for(fragvec_vec::const_iterator i = groupMemFrags.begin();
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
    for(frag_vec::const_iterator i = oldMemFrags.begin();
        i != oldMemFrags.end(); ++i)
    {
        addMemoryFragment(*i);
    }
    oldMemFrags.clear();

    // Apply new locality groups to all Tablets
    for(tablet_vec::iterator i = tablets.begin();
        i != tablets.end(); ++i)
    {
        (*i)->applySchema(schema);
    }
}
