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

#ifndef KDI_SERVER_TABLET_H
#define KDI_SERVER_TABLET_H

#include <kdi/server/Fragment.h>
#include <warp/interval.h>
#include <warp/Runnable.h>
#include <warp/heap.h>
#include <boost/noncopyable.hpp>
#include <string>
#include <vector>
#include <cassert>

namespace kdi {
namespace server {

    class Tablet;

    enum TabletState {
        TABLET_CONFIG_LOADING,
        TABLET_LOG_REPLAYING,
        TABLET_CONFIG_SAVING,
        TABLET_FRAGMENTS_LOADING,
        TABLET_ACTIVE,
        TABLET_UNLOAD_COMPACTING,
        TABLET_UNLOAD_CONFIG_SAVING,
        TABLET_UNLOADED,

        TABLET_ERROR
    };

    // Forward declarations
    class TableSchema;
    class TabletConfig;

    typedef boost::shared_ptr<TabletConfig const> TabletConfigCPtr;

} // namespace server
} // namespace kdi

namespace warp { class Callback; }

//----------------------------------------------------------------------------
// Tablet
//----------------------------------------------------------------------------
class kdi::server::Tablet
    : private boost::noncopyable
{
public:
    explicit Tablet(warp::IntervalPoint<std::string> const & lastRow);
    ~Tablet();

    std::string getTabletName() const;

    warp::IntervalPoint<std::string> const & getFirstRow() const
    {
        return firstRow;
    }

    warp::IntervalPoint<std::string> const & getLastRow() const
    {
        return lastRow;
    }

    warp::Interval<std::string> getRows() const
    {
        return warp::Interval<std::string>(firstRow, lastRow);
    }

    size_t getChainLength(int groupIndex) const
    {
        return fragGroups[groupIndex].size();
    }

    void getFragments(std::vector<FragmentCPtr> & out, int groupIndex) const
    {
        out.insert(out.end(),
                   fragGroups[groupIndex].begin(),
                   fragGroups[groupIndex].end());
    }

    void addFragment(FragmentCPtr const & frag, int groupIndex)
    {
        fragGroups[groupIndex].push_back(frag);
    }

    void replaceFragments(std::vector<FragmentCPtr> const & oldFragments,
                          FragmentCPtr const & newFragment,
                          int groupIndex)
    {
        assert(!oldFragments.empty());

        frag_vec & chain = fragGroups[groupIndex];
        frag_vec::iterator i = std::search(
            chain.begin(), chain.end(),
            oldFragments.begin(), oldFragments.end());
        if(i == chain.end())
            return;

        frag_vec::iterator end = i + oldFragments.size();
        if(newFragment)
        {
            *i = newFragment;
            ++i;
        }
        
        chain.erase(i, end);
    }

    void applySchema(TableSchema const & schema);

    /// Apply Tablet lower bound from config and make any necessary
    /// placeholder fragments.
    void onConfigLoaded(TabletConfigCPtr const & config);

    /// Replace placeholder fragments with real loaded Fragments.
    void onFragmentsLoaded(TableSchema const & schema,
                           std::vector<FragmentCPtr> const & fragments);

    /// Fill in the fragments part of the TabletConfig.
    void getConfigFragments(TabletConfig & cfg) const;
    
    /// Requests a callback when the Tablet load state reaches or
    /// exceeds the given state.
    void deferUntilState(warp::Callback * cb, TabletState state);

    /// Get the current Tablet load state
    TabletState getState() const { return state; }

    /// Set a new Tablet load state.  If any callbacks need to be
    /// issued because of the state change, a Runnable object will be
    /// returned.  The caller must ensure that the object is run.
    warp::Runnable * setState(TabletState state);

    /// Decide if the Tablet is large enough to split.  If so, split
    /// off the lower half of the Tablet and return it.
    Tablet * maybeSplit();

    /// Get the partial data size from all fragments in this tablet
    /// over the given row range.
    size_t getPartialDataSize(
        warp::Interval<std::string> const & rows) const;

private:
    typedef std::vector<FragmentCPtr> frag_vec;
    typedef std::vector<frag_vec> fragvec_vec;

    typedef std::pair<TabletState,warp::Callback *> state_cb;
    typedef warp::MinHeap<state_cb> cb_heap;

private:
    warp::IntervalPoint<std::string> const lastRow;
    warp::IntervalPoint<std::string> firstRow;
    TabletState state;
    fragvec_vec fragGroups;

    // Can probably optimize these out later for better memory
    // efficiency
    TabletConfigCPtr loadingConfig;
    cb_heap cbHeap;
};

#endif // KDI_SERVER_TABLET_H
