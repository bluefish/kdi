//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-08-19
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

#ifndef KDI_TABLET_WORKQUEUE_H
#define KDI_TABLET_WORKQUEUE_H

#include <warp/syncqueue.h>
#include <boost/noncopyable.hpp>
#include <boost/thread.hpp>
#include <boost/function.hpp>

namespace kdi {
namespace tablet {

    class WorkQueue;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// WorkQueue
//----------------------------------------------------------------------------
class kdi::tablet::WorkQueue
    : private boost::noncopyable
{
    typedef boost::function<void ()> job_t;

    warp::SyncQueue<job_t> jobs;
    boost::thread_group threads;
    bool done;

public:
    explicit WorkQueue(size_t nThreads);
    ~WorkQueue();

    void post(job_t const & job);
    void shutdown();

private:
    void workLoop();
};

#endif // KDI_TABLET_WORKQUEUE_H
