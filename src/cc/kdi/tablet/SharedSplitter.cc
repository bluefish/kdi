//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-08-06
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

#include <kdi/tablet/SharedSplitter.h>
#include <kdi/tablet/SuperTablet.h>
#include <boost/bind.hpp>
#include <warp/log.h>

using namespace kdi::tablet;
using warp::log;

//----------------------------------------------------------------------------
// SharedSplitter
//----------------------------------------------------------------------------
SharedSplitter::SharedSplitter()
{
    thread.reset(
        new boost::thread(
            boost::bind(
                &SharedSplitter::splitLoop,
                this
                )
            )
        );
}

SharedSplitter::~SharedSplitter()
{
    shutdown();
}

void SharedSplitter::enqueueSplit(SuperTabletPtr const & super, Tablet * tablet)
{
    queue.push(std::make_pair(super, tablet));
}

void SharedSplitter::shutdown()
{
    if(!thread)
        return;

    queue.cancelWaits();
    thread->join();
    thread.reset();
}    

void SharedSplitter::splitLoop()
{
    log("Split thread starting");

    std::pair<SuperTabletPtr, Tablet *> x;
    while(queue.pop(x))
    {
        x.first->performSplit(x.second);
    }

    log("Split thread exiting");
}
