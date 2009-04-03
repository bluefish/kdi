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

    warp::IntervalPoint<std::string> const & getMinRow() const
    {
        return rows.getLowerBound();
    }
    
    warp::IntervalPoint<std::string> const & getMaxRow() const
    {
        return rows.getUpperBound();
    }

    bool isLoading() const
    {
        return loading.get();
    }

    void getFragments(std::vector<Fragment const *> & out) const
    {
        for(std::vector<FragmentCPtr>::const_iterator i = fragments.begin();
            i != fragments.end(); ++i)
        {
            out.push_back(i->get());
        }
    }

    void deferUntilLoaded(LoadedCb * cb);
    
    void addFragment(FragmentCPtr const & frag);
    warp::Runnable * finishLoading();

private:
    class Loading;

private:
    warp::Interval<std::string> const rows;
    std::vector<FragmentCPtr> fragments;
    std::auto_ptr<Loading> loading;
};

#endif // KDI_SERVER_TABLET_H
