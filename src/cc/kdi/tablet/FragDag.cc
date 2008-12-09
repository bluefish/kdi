//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-12-08
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

#include <kdi/tablet/FragDag.h>
#include <kdi/tablet/Tablet.h>
#include <kdi/tablet/Fragment.h>
#include <utility>
#include <algorithm>
#include <ex/exception.h>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace ex;
using namespace std;

//----------------------------------------------------------------------------
// FragDag
//----------------------------------------------------------------------------
void
FragDag::addFragment(Tablet * tablet, FragmentPtr const & fragment)
{
    FragmentPtr & tail = tailMap[tablet];
    if(tail)
    {
        childMap[tail].insert(fragment);
        parentMap[fragment].insert(tail);
    }
    tail = fragment;
    activeTablets[fragment].insert(tablet);
    activeFragments[tablet].insert(fragment);
}

void FragDag::removeTablet(Tablet * tablet)
{
    fragment_set const & fragments = activeFragments[tablet];
    for(fragment_set::const_iterator f = fragments.begin();
        f != fragments.end(); ++f)
    {
        activeTablets[*f].erase(tablet);
    }

    activeFragments.erase(tablet);
    tailMap.erase(tablet);
}

FragmentPtr
FragDag::getMaxWeightFragment(size_t minWeight) const
{
    // Find fragment with max weight
    FragmentPtr maxFrag;
    size_t maxWeight = minWeight;
    for(ftset_map::const_iterator i = activeTablets.begin();
        i != activeTablets.end(); ++i)
    {
        FragmentPtr const & frag = i->first;
        tablet_set const & tablets = i->second;

        // Frag weight is the sum of the lengths (minus 1) of the
        // tablet chains of which it is a part
        size_t weight = 0;
        for(tablet_set::const_iterator j = tablets.begin();
            j != tablets.end(); ++j)
        {
            Tablet * tablet = *j;
            // Use (length - 1) because we're only interested in
            // compacting chains with two or more fragments
            weight += activeFragments.find(tablet)->second.size() - 1;
        }

        // Update max fragment if this one is bigger
        if(weight > maxWeight)
        {
            maxWeight = weight;
            maxFrag = frag;
        }
    }
    return maxFrag;
}

IntervalSet<string>
FragDag::getActiveRanges(FragmentPtr const & frag) const
{
    IntervalSet<string> rows;
    ftset_map::const_iterator i = activeTablets.find(frag);
    if(i == activeTablets.end())
        raise<RuntimeError>("fragment not in graph");

    tablet_set const & tablets = i->second;
    for(tablet_set::const_iterator j = tablets.begin();
        j != tablets.end(); ++j)
    {
        rows.add((*j)->getRows());
    }
    return rows;
}

size_t
FragDag::getActiveSize(FragmentPtr const & frag) const
{
    IntervalSet<string> rows = getActiveRanges(frag);

    size_t activeSize = 0;
    for(IntervalSet<string>::const_iterator j = rows.begin();
        j != rows.end();)
    {
        IntervalPoint<string> const & lo = *j;  ++j;
        IntervalPoint<string> const & hi = *j;  ++j;

        activeSize += frag->getDiskSize(Interval<string>(lo, hi));
    }

    return activeSize;
}

FragDag::fragment_set
FragDag::getParentSet(fragment_set const & frags) const
{
    fragment_set ret;
    for(fragment_set::const_iterator i = frags.begin();
        i != frags.end(); ++i)
    {
        ffset_map::const_iterator pi = parentMap.find(*i);
        if(pi == parentMap.end())
            raise<RuntimeError>("fragment not in graph");

        fragment_set const & parents = pi->second;
        for(fragment_set::const_iterator j = parents.begin();
            j != parents.end(); ++j)
        {
            // Add parent to adjecent set if it is not already in
            // original set
            if(frags.find(*j) == frags.end())
                ret.insert(*j);
        }
    }
    return ret;
}

FragDag::fragment_set
FragDag::getChildSet(fragment_set const & frags) const
{
    fragment_set ret;
    for(fragment_set::const_iterator i = frags.begin();
        i != frags.end(); ++i)
    {
        ffset_map::const_iterator pi = childMap.find(*i);
        if(pi == childMap.end())
            raise<RuntimeError>("fragment not in graph");

        fragment_set const & childs = pi->second;
        for(fragment_set::const_iterator j = childs.begin();
            j != childs.end(); ++j)
        {
            // Add child to adjecent set if it is not already in
            // original set
            if(frags.find(*j) == frags.end())
                ret.insert(*j);
        }
    }
    return ret;
}

FragDag::fragment_set
FragDag::getAdjacentSet(fragment_set const & frags) const
{
    fragment_set adjacent;

    fragment_set parents = getParentSet(frags);
    fragment_set children = getChildSet(frags);

    adjacent.insert(parents.begin(), parents.end());
    adjacent.insert(children.begin(), children.end());

    return adjacent;
}

IntervalPoint<string>
FragDag::findNextSplit(fragment_vec const & fragments,
                       strref_t minRow)
{
    IntervalPoint<string> splitPoint(string(), PT_INFINITE_UPPER_BOUND);
    IntervalPointOrder<warp::less> plt;

    tablet_set tablets = getActiveTablets(fragments);
    for(tablet_set::const_iterator t = tablets.begin();
        t != tablets.end(); ++t)
    {
        IntervalPoint<string> const & upper =
            (*t)->getRows().getUpperBound();
        if(plt(minRow, upper) && plt(upper, splitPoint))
            splitPoint = upper;
    }

    return splitPoint;
}

FragDag::fragment_vec
FragDag::filterTabletFragments(fragment_vec const & fragments,
                               Tablet * tablet) const
{
    fragment_vec filtered;

    for(fragment_vec::const_iterator f = fragments.begin();
        f != fragments.end(); ++f)
    {
        ftset_map::const_iterator ti = activeTablets.find(*f);
        if(ti == activeTablets.end())
            raise<RuntimeError>("fragment not in graph");

        tablet_set const & tablets = ti->second;
        if(tablets.find(tablet) != tablets.end())
            filtered.push_back(*f);
    }

    return filtered;
}

FragmentPtr
FragDag::getParent(FragmentPtr const & fragment,
                   Tablet * tablet)
{
    ffset_map::const_iterator pi = parentMap.find(fragment);
    if(pi == parentMap.end())
        raise<RuntimeError>("fragment not in graph");

    fragment_set const & parents = pi->second;
    for(fragment_set::const_iterator f = parents.begin();
        f != parents.end(); ++f)
    {
        tablet_set const & tablets = activeTablets.find(*f)->second;
        if(tablets.find(tablet) != tablets.end())
            return *f;
    }
    return FragmentPtr();
}

FragmentPtr
FragDag::getChild(FragmentPtr const & fragment,
                  Tablet * tablet)
{
    ffset_map::const_iterator pi = childMap.find(fragment);
    if(pi == childMap.end())
        raise<RuntimeError>("fragment not in graph");

    fragment_set const & children = pi->second;
    for(fragment_set::const_iterator f = children.begin();
        f != children.end(); ++f)
    {
        tablet_set const & tablets = activeTablets.find(*f)->second;
        if(tablets.find(tablet) != tablets.end())
            return *f;
    }
    return FragmentPtr();
}

FragDag::tablet_set
FragDag::getActiveTablets(fragment_vec const & fragments)
{
    tablet_set active;
    for(fragment_vec::const_iterator f = fragments.begin();
        f != fragments.end(); ++f)
    {
        ftset_map::const_iterator ti = activeTablets.find(*f);
        if(ti == activeTablets.end())
            raise<RuntimeError>("fragment not in graph");

        tablet_set const & tablets = ti->second;
        active.insert(tablets.begin(), tablets.end());
    }
    return active;
}

void
FragDag::replaceFragments(fragment_vec const & fragments,
                          FragmentPtr const & newFragment,
                          Interval<string> const & outputRange)
{
    // Get the contained tablet set
    tablet_set contained;
    {
        tablet_set active = getActiveTablets(fragments);
        for(tablet_set::const_iterator t = active.begin();
            t != active.end(); ++t)
        {
            Interval<string> const & rows = (*t)->getRows();
            if(outputRange.contains(rows))
                contained.insert(*t);

            // XXX implement Interval<T>::overlaps
            // else if(rows.overlaps(outputRange))
            //     raise<RuntimeError>("tablet range overlaps replacement range");
        }
    }

    // Replace fragments for contained tablets
    for(tablet_set::const_iterator t = contained.begin();
        t != contained.end(); ++t)
    {
        fragment_vec filtered = filterTabletFragments(fragments, *t);
        if(filtered.empty())
            continue;

        FragmentPtr parent = getParent(filtered.front(), *t);
        FragmentPtr child = getChild(filtered.back(), *t);

        // Remove fragments and tablets from each other's active
        // sets.
        for(fragment_vec::const_iterator f = filtered.begin();
            f != filtered.end(); ++f)
        {
            activeTablets[*f].erase(*t);
            activeFragments[*t].erase(*f);
        }

        // Add new fragment to active lists
        activeFragments[*t].insert(newFragment);
        activeTablets[newFragment].insert(*t);

        // Update parent-child links
        if(parent)
        {
            parentMap[newFragment].insert(parent);
            childMap[parent].insert(newFragment);
        }
        if(child)
        {
            childMap[newFragment].insert(child);
            parentMap[child].insert(newFragment);
        }

        // Update tail link if we're replacing the tail
        assert((!child) == (filtered.back() == tailMap[*t]));
        if(!child)
        {
            tailMap[*t] = newFragment;
        }

        // Internal graph should be consistent now.  Update the
        // tablet.
        (*t)->replaceFragments(filtered, newFragment);
    }
}

void
FragDag::removeFragments(fragment_vec const & fragments)
{
    // Any tablets still in the active set need to drop these
    // fragments
    tablet_set active = getActiveTablets(fragments);
    for(tablet_set::const_iterator t = active.begin();
        t != active.end(); ++t)
    {
        fragment_vec filtered = filterTabletFragments(fragments, *t);
        if(filtered.empty())
            continue;

        FragmentPtr parent = getParent(filtered.front(), *t);
        FragmentPtr child = getChild(filtered.back(), *t);

        // Remove fragments and tablets from each other's active
        // sets.
        for(fragment_vec::const_iterator f = filtered.begin();
            f != filtered.end(); ++f)
        {
            activeTablets[*f].erase(*t);
            activeFragments[*t].erase(*f);
        }

        // Update parent-child links
        if(parent && child)
        {
            parentMap[child].insert(parent);
            childMap[parent].insert(child);
        }

        // Update tail link if we're removing the tail
        assert((!child) == (filtered.back() == tailMap[*t]));
        if(!child)
        {
            tailMap[*t] = parent;
        }

        // Internal graph should be consistent now.  Update the
        // tablet.
        (*t)->removeFragments(filtered);
    }

    // Remove all childMap references into fragment set
    fragment_set fragset(fragments.begin(), fragments.end());
    fragment_set parents = getParentSet(fragset);
    for(fragment_set::const_iterator p = parents.begin();
        p != parents.end(); ++p)
    {
        for(fragment_vec::const_iterator f = fragments.begin();
            f != fragments.end(); ++f)
        {
            childMap[*p].erase(*f);
        }
    }

    // Remove all parentMap references into fragment set
    fragment_set children = getChildSet(fragset);
    for(fragment_set::const_iterator c = children.begin();
        c != children.end(); ++c)
    {
        for(fragment_vec::const_iterator f = fragments.begin();
            f != fragments.end(); ++f)
        {
            parentMap[*c].erase(*f);
        }
    }

    // Remove fragments from all fragment maps
    for(fragment_vec::const_iterator f = fragments.begin();
        f != fragments.end(); ++f)
    {
        parentMap.erase(*f);
        childMap.erase(*f);
        activeTablets.erase(*f);
    }
}

FragDag::fragment_set
FragDag::chooseCompactionSet() const
{
    fragment_set frags;

    FragmentPtr frag = getMaxWeightFragment(0);
    if(!frag)
        return frags;

    // Expand fragment set until it is sufficiently large
    //  (16-way merge or 2 GB compaction, whichever comes first)
    size_t const SUFFICIENTLY_LARGE = size_t(2) << 30;
    size_t const SUFFICIENTLY_NUMEROUS = 16;
    size_t curSpace = getActiveSize(frag);
    for(;;)
    {
        // Get set of adjacent nodes
        fragment_set adjacent = getAdjacentSet(frags);
        if(adjacent.empty())
            return frags;

        typedef std::pair<size_t, FragmentPtr> size_pair;
        typedef std::vector<size_pair> size_vec;

        // Get sizes of adjacent nodes
        size_vec sizes;
        sizes.reserve(adjacent.size());
        for(fragment_set::const_iterator i = adjacent.begin();
            i != adjacent.end(); ++i)
        {
            size_t sz = getActiveSize(*i);
            sizes.push_back(size_pair(sz, *i));
        }

        // Sort by size
        std::sort(sizes.begin(), sizes.end());

        // Add to fragment set in order of increasing size
        for(size_vec::const_iterator i = sizes.begin();
            i != sizes.end(); ++i)
        {
            frags.insert(i->second);
            curSpace += i->first;

            if(frags.size() >= SUFFICIENTLY_NUMEROUS ||
               curSpace >= SUFFICIENTLY_LARGE)
            {
                return frags;
            }
        }
    }
}

FragDag::fragment_vec
FragDag::chooseCompactionList() const
{
    fragment_vec frags;

    // Get the set of fragments to compact
    fragment_set fset = chooseCompactionSet();
    if(fset.size() < 2)
        return frags;

    // Get fragments in topological order, ancestors first
    while(!fset.empty())
    {
        size_t oldSize = fset.size();
        for(fragment_set::iterator f = fset.begin(); f != fset.end();)
        {
            bool hasParent = false;
            fragment_set const & parents = parentMap.find(*f)->second;
            for(fragment_set::const_iterator p = parents.begin();
                p != parents.end(); ++p)
            {
                if(fset.find(*p) != fset.end())
                {
                    hasParent = true;
                    break;
                }
            }

            if(!hasParent)
            {
                frags.push_back(*f);
                fset.erase(f++);
            }
            else
            {
                ++f;
            }
        }
        if(fset.size() == oldSize)
            raise<RuntimeError>("graph has a cycle");
    }

    return frags;
}

bool
FragDag::isRooted(fragment_vec const & frags) const
{
    return getParentSet(fragment_set(frags.begin(), frags.end())).empty();
}
