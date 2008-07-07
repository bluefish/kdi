//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-21
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

#ifndef KDI_TABLET_SHAREDCOMPACTOR_H
#define KDI_TABLET_SHAREDCOMPACTOR_H

#include <kdi/tablet/forward.h>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <set>

namespace kdi {
namespace tablet {

    class SharedCompactor;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// SharedCompactor
//----------------------------------------------------------------------------
class kdi::tablet::SharedCompactor :
    private boost::noncopyable
{
    typedef std::set<TabletWeakPtr> set_t;
    typedef boost::mutex::scoped_lock lock_t;

    boost::mutex mutex;
    boost::condition requestAdded;
    boost::scoped_ptr<boost::thread> thread;

    set_t requests;
    bool cancel;

public:
    SharedCompactor();
    ~SharedCompactor();

    void requestCompaction(TabletPtr const & tablet);
    void shutdown();

private:
    bool getTabletForCompaction(TabletPtr & tablet);
    void compactLoop();
};

#endif // KDI_TABLET_SHAREDCOMPACTOR_H
