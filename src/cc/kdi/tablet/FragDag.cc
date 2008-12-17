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
#include <warp/log.h>
#include <warp/fs.h>
#include <utility>
#include <algorithm>
#include <sstream>
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
    log("FragDag: Tablet %s append %s",
        tablet->getPrettyName(),
        fragment->getFragmentUri());

    activeTablets[fragment].insert(tablet);
    activeFragments[tablet].insert(fragment);
}

void FragDag::removeTablet(Tablet * tablet)
{
    tfset_map::iterator i = activeFragments.find(tablet);
    if(i == activeFragments.end())
        return;

    log("FragDag: remove tablet %s", tablet->getPrettyName());

    fragment_set const & fragments = i->second;
    for(fragment_set::const_iterator f = fragments.begin();
        f != fragments.end(); ++f)
    {
        tablet_set & tablets = activeTablets[*f];
        tablets.erase(tablet);
        if(tablets.empty())
            removeInactiveFragment(*f);
    }

    activeFragments.erase(i);
}

void FragDag::removeInactiveFragment(FragmentPtr const & fragment)
{
    assert(activeTablets[fragment].empty());

    log("FragDag: remove fragment %s", fragment->getFragmentUri());

    // Remove fragment from active map
    activeTablets.erase(fragment);
}

size_t
FragDag::getFragmentWeight(FragmentPtr const & fragment) const
{
    ftset_map::const_iterator i = activeTablets.find(fragment);
    if(i == activeTablets.end())
        raise<RuntimeError>("fragment not in graph: %s",
                            fragment->getFragmentUri());

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

    return weight;
}

FragmentPtr
FragDag::getMaxWeightFragment(size_t minWeight) const
{
    //log("FragDag: getMaxWeightFragment");

    // Find fragment with max weight
    FragmentPtr maxFrag;
    size_t maxWeight = minWeight;
    size_t totalWeight = 0;
    for(ftset_map::const_iterator i = activeTablets.begin();
        i != activeTablets.end(); ++i)
    {
        FragmentPtr const & frag = i->first;

        size_t weight = getFragmentWeight(frag);
        //log("FragDag: .. weight %d: %s", weight, frag->getFragmentUri());

        // Update max fragment if this one is bigger
        if(weight > maxWeight)
        {
            maxWeight = weight;
            maxFrag = frag;
        }

        // Track total weight as a measure of graph fragmentation
        totalWeight += weight;
    }

    log("FragDag: total graph weight: %d", totalWeight);
    if(!activeFragments.empty())
    {
        size_t totChain = 0;
        size_t maxChain = 0;
        for(tfset_map::const_iterator i = activeFragments.begin();
            i != activeFragments.end(); ++i)
        {
            size_t len = i->second.size();
            totChain += len;
            if(len > maxChain)
                maxChain = len;

        }
        log("FragDag: average chain length: %.2f",
            double(totChain) / activeFragments.size());
        log("FragDag: maximum chain length: %d", maxChain);
    }

    return maxFrag;
}

IntervalSet<string>
FragDag::getActiveRanges(FragmentPtr const & frag) const
{
    IntervalSet<string> rows;
    ftset_map::const_iterator i = activeTablets.find(frag);
    if(i == activeTablets.end())
        raise<RuntimeError>("fragment not in graph: %s",
                            frag->getFragmentUri());

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
    return frag->getDiskSize(getActiveRanges(frag));
}

FragDag::fragment_set
FragDag::getAdjacentSet(fragment_set const & frags) const
{
    fragment_set adjacent;

    for(fragment_set::const_iterator f = frags.begin();
        f != frags.end(); ++f)
    {
        ftset_map::const_iterator i = activeTablets.find(*f);
        if(i == activeTablets.end())
            raise<ValueError>("fragment not in graph: %s",
                              (*f)->getFragmentUri());

        tablet_set const & tablets = i->second;
        for(tablet_set::const_iterator t = tablets.begin();
            t != tablets.end(); ++t)
        {
            FragmentPtr parent = getParent(*f, *t);
            if(parent && frags.find(parent) == frags.end())
                adjacent.insert(parent);

            FragmentPtr child = getChild(*f, *t);
            if(child && frags.find(child) == frags.end())
                adjacent.insert(child);
        }
    }

    return adjacent;
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
            raise<RuntimeError>("fragment not in graph: %s",
                                (*f)->getFragmentUri());

        tablet_set const & tablets = ti->second;
        if(tablets.find(tablet) != tablets.end())
            filtered.push_back(*f);
    }

    return filtered;
}

FragmentPtr
FragDag::getParent(FragmentPtr const & fragment, Tablet * tablet) const
{
    return tablet->getFragmentParent(fragment);
}

FragmentPtr
FragDag::getChild(FragmentPtr const & fragment, Tablet * tablet) const
{
    // Don't return log fragments
    FragmentPtr f = tablet->getFragmentChild(fragment);
    if(activeTablets.find(f) != activeTablets.end())
        return f;
    else
        return FragmentPtr();
}

FragDag::tablet_set
FragDag::getActiveTablets(fragment_set const & fragments) const
{
    tablet_set active;
    for(fragment_set::const_iterator f = fragments.begin();
        f != fragments.end(); ++f)
    {
        ftset_map::const_iterator ti = activeTablets.find(*f);
        if(ti == activeTablets.end())
            raise<RuntimeError>("fragment not in graph: %s",
                                (*f)->getFragmentUri());

        tablet_set const & tablets = ti->second;
        active.insert(tablets.begin(), tablets.end());
    }
    return active;
}

FragDag::tablet_set
FragDag::getActiveTabletIntersection(fragment_vec const & fragments) const
{
    tablet_set active;

    if(fragments.empty())
        return active;

    // Init active set from first element
    fragment_vec::const_iterator f = fragments.begin();
    {
        ftset_map::const_iterator ti = activeTablets.find(*f);
        if(ti == activeTablets.end())
            return active;

        active = ti->second;
    }

    // For each subsequent fragment, erase tablets from the
    // intersection set not in the later fragment's active tablet set
    for(++f; f != fragments.end(); ++f)
    {
        ftset_map::const_iterator ti = activeTablets.find(*f);
        if(ti == activeTablets.end())
        {
            active.clear();
            return active;
        }

        tablet_set const & tablets = ti->second;
        for(tablet_set::iterator t = active.begin();
            t != active.end(); )
        {
            if(tablets.find(*t) == tablets.end())
                active.erase(t++);
            else
                ++t;
        }
    }

    return active;
}

void
FragDag::replaceInternal(Tablet * tablet,
                         fragment_vec const & adjFragments,
                         FragmentPtr const & newFragment)
{
    log("FragDag: updating %s", tablet->getPrettyName());

    assert(!adjFragments.empty());
    assert(filterTabletFragments(adjFragments, tablet).size() == adjFragments.size());

    FragmentPtr parent = getParent(adjFragments.front(), tablet);
    FragmentPtr child = getChild(adjFragments.back(), tablet);

    // Remove fragments and tablet from each other's active sets.
    log("FragDag: parent: %s", parent ? parent->getFragmentUri() : "NULL");
    for(fragment_vec::const_iterator f = adjFragments.begin();
        f != adjFragments.end(); ++f)
    {
        log("FragDag: .. %d: %s", f - adjFragments.begin(), (*f)->getFragmentUri());

        activeTablets[*f].erase(tablet);
        activeFragments[tablet].erase(*f);
    }
    log("FragDag:  child: %s", child ? child->getFragmentUri() : "NULL");

    // Are we doing a replacement or deletion?
    if(newFragment)
    {
        // Replacement: update graph by splicing fragment into place
        log("FragDag: replacing %d fragment(s) with %s",
            adjFragments.size(), newFragment->getFragmentUri());

        // Add new fragment to active lists
        activeFragments[tablet].insert(newFragment);
        activeTablets[newFragment].insert(tablet);

        // Internal graph should be consistent now.  Update the
        // tablet.
        (tablet)->replaceFragments(adjFragments, newFragment);
    }
    else
    {
        // Deletion: update graph by splicing nothing into place
        log("FragDag: removing %d fragment(s)", adjFragments.size());

        // Internal graph should be consistent now.  Update the
        // tablet.
        (tablet)->removeFragments(adjFragments);
    }
}

void
FragDag::replaceFragments(warp::Interval<std::string> const & range,
                          fragment_vec const & adjFragments,
                          FragmentPtr const & newFragment)
{
    // graph::replace(range, adjSeq, outFrag):
    //    for tablet in (intersection of active tablets for each frag in adjSeq):
    //       -- guaranteed tablet contains adjSeq (1)
    //       if range contains tablet.rows:
    //          -- guaranteed we merged all relevant data (2)
    //          if outFrag contains data in tablet.rows:   -- optimization (3)
    //             tablet.replace(adjSeq, outFrag)   -- blow up if adjSeq not found
    //          else:
    //             tablet.remove(adjSeq)             -- blow up if adjSeq not found
    //
    // (1) Guarantee: All of the fragments in adjSeq are part of the
    //     current compaction set, so they are active and can't be
    //     replaced by something different with the same name.  If a
    //     tablet were to get dropped from this server, loaded by
    //     another server, compacted, dropped from that server, and
    //     reloaded by this server all while the compaction on this
    //     server was happening, then we have two cases: 1) at least
    //     one of fragments in the adjSeq was compacted and replaced
    //     for this tablet by another server, or 2) the other server
    //     didn't touch anything in adjSeq.  For 1), when the tablet
    //     gets reloaded it will not be in the active set for the
    //     replaced fragment.  The tablet will not be contained in the
    //     intersection of all active tablets for the fragment.  For
    //     2), we can go ahead with the replacement.
    //
    // (2) Guarantee: The input scan on each fragment in adjSeq
    //     includes all ranges for each tablet referencing the
    //     fragment (in that tablet's adjSeq).  There is no data in
    //     any of the fragments in adjSeq inside of the given range
    //     that is not included in outFrag.  This allows us to be
    //     confident about doing the right thing for recently split or
    //     joined tablets.
    //
    // (3) Optimization: it's possible outFrag doesn't contain
    //     anything in the tablet's range, and therefore doesn't need
    //     to be included in the tablet's fragment chain.  This seems
    //     most likely to happen when tablets split.

    if(adjFragments.empty())
        raise<ValueError>("empty replacement set");

    tablet_set active = getActiveTabletIntersection(adjFragments);
    for(tablet_set::const_iterator t = active.begin();
        t != active.end(); ++t)
    {
        if(!range.contains((*t)->getRows()))
            continue;

        replaceInternal(*t, adjFragments, newFragment);
    }
}

void
FragDag::removeFragments(warp::Interval<std::string> const & range,
                         fragment_vec const & adjFragments)
{
    // See replaceFragments()

    if(adjFragments.empty())
        raise<ValueError>("empty replacement set");

    tablet_set active = getActiveTabletIntersection(adjFragments);
    for(tablet_set::const_iterator t = active.begin();
        t != active.end(); ++t)
    {
        if(!range.contains((*t)->getRows()))
            continue;

        replaceInternal(*t, adjFragments, FragmentPtr());
    }
}

void
FragDag::sweepGraph()
{
    log("FragDag: sweeping graph");

    for(tfset_map::iterator i = activeFragments.begin();
        i != activeFragments.end(); )
    {
        if(i->second.empty())
            removeTablet((i++)->first);
        else
            ++i;
    }

    for(ftset_map::iterator i = activeTablets.begin();
        i != activeTablets.end(); )
    {
        if(i->second.empty())
            removeInactiveFragment((i++)->first);
        else
            ++i;
    }
}

FragDag::fragment_set
FragDag::chooseCompactionSet() const
{
    log("FragDag: chooseCompactionSet");
    fragment_set frags;

    FragmentPtr frag = getMaxWeightFragment(0);
    if(!frag)
    {
        log("FragDag: no max weight fragment");
        return frags;
    }

    log("FragDag: max fragment: %s", frag->getFragmentUri());

    // Expand fragment set until it is sufficiently large
    //  (16-way merge or 4 GB compaction, whichever comes first)
    size_t const SUFFICIENTLY_LARGE = size_t(4) << 30;
    size_t const SUFFICIENTLY_NUMEROUS = 16;
    size_t curSpace = getActiveSize(frag);
    frags.insert(frag);
    for(;;)
    {
        log("FragDag: expanding set: size=%d, space=%s",
            frags.size(), sizeString(curSpace));

        // Get set of adjacent nodes
        fragment_set adjacent = getAdjacentSet(frags);
        log("FragDag: found %d neighbor(s)", adjacent.size());
        if(adjacent.empty())
        {
            log("FragDag: compaction set is complete");
            return frags;
        }

        typedef std::pair<size_t, FragmentPtr> size_pair;
        typedef std::vector<size_pair> size_vec;

        // Get weights of adjacent nodes
        size_vec sizes;
        sizes.reserve(adjacent.size());
        for(fragment_set::const_iterator i = adjacent.begin();
            i != adjacent.end(); ++i)
        {
            size_t wt = getFragmentWeight(*i);
            sizes.push_back(size_pair(wt, *i));
            //log("FragDag: .. weight %d: %s", wt, (*i)->getFragmentUri());
        }

        // Sort by size
        std::sort(sizes.begin(), sizes.end(), std::greater<size_pair>());

        // Add to fragment set in order of decreasing weight
        for(size_vec::const_iterator i = sizes.begin();
            i != sizes.end(); ++i)
        {
            frags.insert(i->second);
            curSpace += getActiveSize(i->second);

            log("FragDag: grow set (size=%d, space=%s): %s",
                frags.size(), sizeString(curSpace),
                i->second->getFragmentUri());

            if(frags.size() >= SUFFICIENTLY_NUMEROUS ||
               curSpace >= SUFFICIENTLY_LARGE)
            {
                log("FragDag: compaction set is sufficiently large");
                return frags;
            }

            // Only add one node before getting a new expansion set.
            break;
        }
    }
}

bool
FragDag::hasParent(FragmentPtr const & fragment,
                   fragment_set const & inSet) const
{
    ftset_map::const_iterator i = activeTablets.find(fragment);
    if(i == activeTablets.end())
        raise<RuntimeError>("not in graph: %s",
                            fragment->getFragmentUri());

    tablet_set const & tablets = i->second;
    for(tablet_set::const_iterator t = tablets.begin();
        t != tablets.end(); ++t)
    {
        FragmentPtr parent = getParent(fragment, *t);
        if(!parent)
            continue;

        if(inSet.find(parent) != inSet.end())
            return true;
    }

    return false;
}

void
FragDag::dumpDotGraph(std::ostream & out,
                      fragment_set const & fragments,
                      bool restrictToSet) const
{
    out << "digraph G {" << endl
        << "rankdir=LR;" << endl;

    fragment_set external;

    for(fragment_set::const_iterator f = fragments.begin();
        f != fragments.end(); ++f)
    {
        out << "// fragment: " << (*f)->getFragmentUri() << endl;

        ftset_map::const_iterator i = activeTablets.find(*f);
        if(i == activeTablets.end())
        {
            out << "// not in graph: " << (*f)->getFragmentUri() << endl;
            out << fs::basename((*f)->getFragmentUri())
                << " [shape=oval,color=red];" << endl;
            continue;
        }

        out << fs::basename((*f)->getFragmentUri())
            << " [shape=oval,color=green];" << endl;

        tablet_set const & tablets = i->second;

        // Scan for parent links
        fragment_set linked;
        for(tablet_set::const_iterator t = tablets.begin();
            t != tablets.end(); ++t)
        {
            // Find the parent of the fragment in this tablet chain
            FragmentPtr parent = getParent(*f, *t);

            out << "//   in tablet: " << (*t)->getPrettyName() << endl;

            // No parent, no link
            if(!parent)
            {
                out << "//     is root" << endl;
                continue;
            }

            out << "//     parent: " << parent->getFragmentUri() << endl;

            // Check to see if the parent is in the fragment set
            if(fragments.find(parent) == fragments.end())
            {
                // If we don't want nodes outside the fragment set,
                // skip.  Otherwhise note that we've found an external
                // node.
                if(restrictToSet)
                    continue;
                else
                    external.insert(parent);
            }

            // If we've already linked the parent to the fragment,
            // don't do it again
            if(linked.find(parent) != linked.end())
                continue;

            // Make a link from the parent to the fragment
            out << fs::basename(parent->getFragmentUri())
                << " -> "
                << fs::basename((*f)->getFragmentUri())
                << ";" << endl;

            // Remember the parent so we don't duplicate links
            linked.insert(parent);
        }

        // Don't bother with child links if we don't want things
        // outside the set
        if(restrictToSet)
            continue;

        // Scan again for child links
        linked.clear();
        for(tablet_set::const_iterator t = tablets.begin();
            t != tablets.end(); ++t)
        {
            // Find the child of the fragment in this tablet chain
            FragmentPtr child = getChild(*f, *t);

            out << "//   in tablet: " << (*t)->getPrettyName() << endl;

            // No child, no link
            if(!child)
            {
                out << "//     is leaf" << endl;
                continue;
            }

            out << "//     child: " << child->getFragmentUri() << endl;

            // If the child is in the fragment set, it will get a
            // parent link elsewhere
            if(fragments.find(child) != fragments.end())
                continue;

            // If we've already linked the fragment to the child,
            // don't do it again
            if(linked.find(child) != linked.end())
                continue;

            // Make a link from the fragment to the child
            out << fs::basename((*f)->getFragmentUri())
                << " -> "
                << fs::basename(child->getFragmentUri())
                << ";" << endl;

            // Add the child to the external set
            external.insert(child);

            // Remember the child so we don't duplicate links
            linked.insert(child);
        }
    }

    // Make all the external fragments look different
    for(fragment_set::const_iterator f = external.begin();
        f != external.end(); ++f)
    {
        out << fs::basename((*f)->getFragmentUri())
            << " [shape=box,color=blue];" << endl;
    }

    out << "}" << endl;
}
