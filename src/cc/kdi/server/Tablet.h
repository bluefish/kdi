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
#include <boost/noncopyable.hpp>
#include <memory>
#include <string>
#include <vector>

namespace kdi {
namespace server {

    class Tablet;

    enum TabletState {
        TABLET_UNKNOWN,
        TABLET_CONFIG_LOADING,
        TABLET_LOG_REPLAYING,
        TABLET_CONFIG_SAVING,
        TABLET_FRAGMENTS_LOADING,
        TABLET_ACTIVE,
        TABLET_UNLOAD_COMPACTING,
        TABLET_UNLOAD_CONFIG_SAVING,
    };

    // Forward declarations
    class TableSchema;
    class TabletConfig;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// Tablet
//----------------------------------------------------------------------------
class kdi::server::Tablet
    : private boost::noncopyable
{
public:
    class LoadedCb
    {
    public:
        virtual void done() = 0;
        
    protected:
        ~LoadedCb() {}
    };

public:
    /// Create a Tablet over the given row interval.  The newly
    /// created tablet will be in the "loading" state.
    explicit Tablet(warp::Interval<std::string> const & rows);
    ~Tablet();

    warp::Interval<std::string> const & getRows() const
    {
        return rows;
    }

    bool isLoading() const
    {
        return loading.get();
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
    void deferUntilLoaded(LoadedCb * cb);
    warp::Runnable * finishLoading();

    /// Fill in the fragments part of the TabletConfig.
    void getConfigFragments(TabletConfig & cfg) const;

private:
    class Loading;

    typedef std::vector<FragmentCPtr> frag_vec;
    typedef std::vector<frag_vec> fragvec_vec;

private:
    TabletState state;
    warp::Interval<std::string> const rows;
    fragvec_vec fragGroups;
    std::auto_ptr<Loading> loading;
};

#endif // KDI_SERVER_TABLET_H
