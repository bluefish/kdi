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

#include <kdi/server/Tablet.h>

using namespace kdi;
using namespace kdi::server;

//----------------------------------------------------------------------------
// Tablet::Loading
//----------------------------------------------------------------------------
class Tablet::Loading
    : public warp::Runnable
{
public:
    std::vector<LoadedCb *> callbacks;

    void run()
    {
        for(std::vector<LoadedCb *>::const_iterator i = callbacks.begin();
            i != callbacks.end(); ++i)
        {
            (*i)->done();
        }
        delete this;
    }
};


//----------------------------------------------------------------------------
// Tablet
//----------------------------------------------------------------------------
Tablet::Tablet(warp::Interval<std::string> const & rows) :
    rows(rows),
    loading(new Loading)
{
}

Tablet::~Tablet()
{
}

void Tablet::deferUntilLoaded(LoadedCb * cb)
{
    assert(loading.get());
    loading->callbacks.push_back(cb);
}

void Tablet::addFragment(FragmentCPtr const & frag)
{
    fragments.push_back(frag);
}

warp::Runnable * Tablet::finishLoading()
{
    assert(loading.get());

    if(!loading->callbacks.empty())
        return loading.release();

    loading.reset();
    return 0;
}

