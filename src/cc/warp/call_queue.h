//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-02-10
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef WARP_CALL_QUEUE_H
#define WARP_CALL_QUEUE_H

#include <warp/heap.h>
#include <boost/noncopyable.hpp>
#include <boost/function.hpp>
#include <boost/thread.hpp>

namespace warp {

    class CallQueue;

} // namespace warp

//----------------------------------------------------------------------------
// CallQueue
//----------------------------------------------------------------------------
class warp::CallQueue
    : private boost::noncopyable
{
    typedef boost::mutex mutex_t;
    typedef mutex_t::scoped_lock lock_t;

    struct Item
    {
        int64_t when;
        boost::function<void ()> call;

        bool operator<(Item const & o) const {
            return when < o.when;
        }
    };

    boost::thread_group threads;
    MinHeap<Item> items;

    mutex_t mutex;
    boost::condition wakeup;
    bool exit;

    void threadLoop();

public:
    explicit CallQueue(size_t nThreads);
    ~CallQueue();

    void callLater(double delaySeconds,
                   boost::function<void ()> const & call);
};


#endif // WARP_CALL_QUEUE_H
