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

#ifndef KDI_TABLET_SHAREDSPLITTER_H
#define KDI_TABLET_SHAREDSPLITTER_H

#include <kdi/tablet/forward.h>
#include <warp/syncqueue.h>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <utility>

namespace kdi {
namespace tablet {

    /// Single thread of execution for splitting Tablets
    class SharedSplitter;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// SharedSplitter
//----------------------------------------------------------------------------
class kdi::tablet::SharedSplitter
{
    warp::SyncQueue< std::pair<SuperTabletPtr, Tablet *> > queue;
    boost::scoped_ptr<boost::thread> thread;

public:
    SharedSplitter();
    ~SharedSplitter();

    void enqueueSplit(SuperTabletPtr const & super, Tablet * tablet);
    void shutdown();

private:
    void splitLoop();
};


#endif // KDI_TABLET_SHAREDSPLITTER_H
