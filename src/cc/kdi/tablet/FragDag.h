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

#ifndef KDI_TABLET_FRAGDAG_H
#define KDI_TABLET_FRAGDAG_H

#include <kdi/tablet/forward.h>
#include <warp/interval.h>
#include <warp/string_range.h>
#include <string>
#include <ostream>
#include <vector>
#include <set>
#include <map>

namespace kdi {
namespace tablet {

    class FragDag;
    class CompactionList;

} // namespace tablet
} // namespace kdi

class kdi::tablet::CompactionList
{
public:
    Tablet const * tablet;
    std::vector<FragmentPtr> fragments;
};

//----------------------------------------------------------------------------
// FragDag
//----------------------------------------------------------------------------
class kdi::tablet::FragDag
{
public:
    typedef std::vector<FragmentPtr>              fragment_vec;
    typedef std::set<FragmentPtr>                 fragment_set;
    typedef std::set<Tablet*>                     tablet_set;
    typedef std::map<FragmentPtr, tablet_set>     ftset_map;
    typedef std::map<Tablet*, fragment_set>       tfset_map;

private:
    ftset_map activeTablets;
    tfset_map activeFragments;

    struct FragStats {
        size_t diskSize;
        size_t activeDiskSize;
        size_t nStartingTablets; 
    };

    typedef std::map<FragmentPtr, FragStats> fstat_map;
    mutable fstat_map activeFragStats;

    FragStats const &
    getFragmentStats(FragmentPtr const & fragment) const;

    size_t const
    getInactiveSize(FragmentPtr const & fragment) const;

    fragment_set
    getMostWastedSet(size_t maxSize) const;

public:
    /// Append an edge to a tablet's fragment list.
    void
    addFragment(Tablet * tablet, FragmentPtr const & fragment);

    /// Remove all references to tablet from DAG.
    void removeTablet(Tablet * tablet);

private:
    /// Remove an inactive fragment from DAG.
    void removeInactiveFragment(FragmentPtr const & fragment);

public:
    size_t
    getFragmentWeight(FragmentPtr const & fragment) const;

    /// XXX ...
    FragmentPtr
    getMaxWeightFragment(size_t minWeight) const;

    /// XXX ...
    warp::IntervalSet<std::string>
    getActiveRanges(FragmentPtr const & frag) const;

    /// XXX ...
    size_t
    getActiveSize(FragmentPtr const & frag) const;

private:
    /// Get all immediate parents and children not already included in
    /// the set
    fragment_set
    getAdjacentSet(fragment_set const & frags) const;

public:
    // Filter fragment list based on which belong to a given tablet's
    // active set
    fragment_vec
    filterTabletFragments(fragment_vec const & fragments,
                          Tablet * tablet) const;

    /// Get the parent of the given fragment in the line of the given
    /// tablet.
    FragmentPtr
    getParent(FragmentPtr const & fragment, Tablet const * tablet) const;

    /// Get the child of the given fragment in the line of the given
    /// tablet.
    FragmentPtr
    getChild(FragmentPtr const & fragment, Tablet * tablet) const;

    /// XXX ...
    tablet_set
    getActiveTablets(fragment_set const & fragments) const;

    tablet_set
    getActiveTabletIntersection(fragment_vec const & fragments) const;

private:
    /// Replace the fragments in adjFragments with newFragment for the
    /// given tablet.  adjFragments must be a non-empty, adjacent
    /// sequence in tablet's active list of fragments.  If newFragment
    /// is not null, it is spliced into the place of adjFragments.  If
    /// newFragment is null, adjFragments are spliced out.
    void
    replaceInternal(Tablet * tablet,
                    fragment_vec const & adjFragments,
                    FragmentPtr const & newFragment);

public:
    /// Replace the given fragments in active tablets contained in
    /// range with newFragment.  adjFragments is expected to be an
    /// adjacent sequence of fragments in each matching tablet's
    /// active fragment list.
    void
    replaceFragments(warp::Interval<std::string> const & range,
                     fragment_vec const & adjFragments,
                     FragmentPtr const & newFragment);

    /// Remove the given fragments from active tablets contained in
    /// range.  adjFragments is expected to be an adjacent sequence of
    /// fragments in each matching tablet's active fragment list.
    void
    removeFragments(warp::Interval<std::string> const & range,
                    fragment_vec const & adjFragments);

    /// Sweep graph of inactive tablets and fragments.  A tablet is
    /// inactive if it has no active fragments and vice-versa.
    void sweepGraph();

    /// XXX ...
    std::vector<kdi::tablet::CompactionList>
    chooseCompactionSet() const;

    void
    dumpDotGraph(std::ostream & out,
                 fragment_set const & fragments,
                 bool restrictToSet) const;
private:
    // Configurable paramaters for compaction set algorithm
    size_t MAX_COMPACTION_WIDTH;

public:
    FragDag();
};

#endif // KDI_TABLET_FRAGDAG_H
