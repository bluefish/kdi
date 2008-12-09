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
#include <vector>
#include <set>
#include <map>

namespace kdi {
namespace tablet {

    class FragDag;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// FragDag
//----------------------------------------------------------------------------
class kdi::tablet::FragDag
{
    typedef std::vector<FragmentPtr>              fragment_vec;
    typedef std::set<FragmentPtr>                 fragment_set;
    typedef std::set<TabletPtr>                   tablet_set;
    typedef std::map<FragmentPtr, fragment_set>   ffset_map;
    typedef std::map<FragmentPtr, tablet_set>     ftset_map;
    typedef std::map<TabletPtr, fragment_set>     tfset_map;
    typedef std::map<TabletPtr, FragmentPtr>      tf_map;

    ffset_map parentMap;
    ffset_map childMap;
    ftset_map activeTablets;
    tfset_map activeFragments;
    tf_map tailMap;

public:
    /// Append an edge to a tablet's fragment list.
    void
    addFragment(TabletPtr const & tablet, FragmentPtr const & fragment);

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
    /// Get all immediate parents not already included in the set
    fragment_set
    getParentSet(fragment_set const & frags) const;

    /// Get all immediate childs not already included in the set
    fragment_set
    getChildSet(fragment_set const & frags) const;

    /// Get all immediate parents and children not already included in
    /// the set
    fragment_set
    getAdjacentSet(fragment_set const & frags) const;

public:
    // Find the first tablet boundary not before minRow
    warp::IntervalPoint<std::string>
    findNextSplit(fragment_vec const & fragments, warp::strref_t minRow);

    // Filter fragment list based on which belong to a given tablet's
    // active set
    fragment_vec
    filterTabletFragments(fragment_vec const & fragments,
                          TabletPtr const & tablet) const;

    /// Get the parent of the given fragment in the line of the given
    /// tablet.
    FragmentPtr
    getParent(FragmentPtr const & fragment,
              TabletPtr const & tablet);

    /// Get the child of the given fragment in the line of the given
    /// tablet.
    FragmentPtr
    getChild(FragmentPtr const & fragment,
             TabletPtr const & tablet);

    tablet_set
    getActiveTablets(fragment_vec const & fragments);

    /// Replace the list of fragments in each tablet contained in
    /// fragmentRows with newFragment.  Tablets covering ranges
    /// outside fragmentRows will not be modified.
    void
    replaceFragments(fragment_vec const & fragments,
                     FragmentPtr const & newFragment,
                     warp::Interval<std::string> const & outputRange);

    // Drop the fragments from the graph
    void
    removeFragments(fragment_vec const & fragments);

    /// XXX ...
    fragment_set
    chooseCompactionSet() const;

    /// XXX ...
    fragment_vec
    chooseCompactionList() const;

    // A fragment set is rooted if all parents of nodes in the set are
    // also in the set
    bool
    isRooted(fragment_vec const & frags) const;
};

#endif // KDI_TABLET_FRAGDAG_H
