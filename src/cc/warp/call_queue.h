//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/call_queue.h $
//
// Created 2008/02/10
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
