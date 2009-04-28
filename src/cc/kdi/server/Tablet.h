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

    enum TabletState { TABLET_ACTIVE };

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

    void applySchema(TableSchema const & schema);
    void deferUntilLoaded(LoadedCb * cb);
    warp::Runnable * finishLoading();

    /// Fill in the rows and fragments part of the TabletConfig.
    void fillConfig(TabletConfig & cfg) const;

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
