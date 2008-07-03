//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-03-26
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

#ifndef WARP_SYNCQUEUE_H
#define WARP_SYNCQUEUE_H

#include <queue>
#include <boost/thread.hpp>
#include <stddef.h>

namespace warp
{
    template <class T>
    class SyncQueue;
}

//----------------------------------------------------------------------------
// SyncQueue
//----------------------------------------------------------------------------
template <class T>
class warp::SyncQueue : private boost::noncopyable
{
    typedef boost::mutex mutex_t;
    typedef mutex_t::scoped_lock lock_t;
    typedef boost::condition condition_t;
    typedef std::queue<T> queue_t;
    typedef typename queue_t::value_type value_t;
    typedef SyncQueue<T> my_t;

    // Queue data
    queue_t q;

    // Synchronization objects
    size_t qMax;
    mutable mutex_t qMutex;
    condition_t itemPushed;
    condition_t itemPopped;
    condition_t zeroWaiting;
    condition_t taskCompleted;
    size_t nWaiting;
    size_t nTasksPending;
    bool cancel;

    // Untimed wait helper
    void waitHelper(lock_t & lock, condition_t & cond)
    {
        ++nWaiting;
        cond.wait(lock);
        if(--nWaiting == 0)
            zeroWaiting.notify_all();
    }

    // Timed wait helper
    bool waitHelper(lock_t & lock, condition_t & cond, boost::xtime const & xt)
    {
        ++nWaiting;
        bool gotCond = cond.timed_wait(lock, xt);
        if(--nWaiting == 0)
            zeroWaiting.notify_all();
        return gotCond;
    }

    // Helper to construct time out
    static boost::xtime getTimeout(double timeout)
    {
        boost::xtime xt;
        boost::xtime_get(&xt, boost::TIME_UTC);

        int64_t sec = int64_t(timeout);
        xt.sec += sec;
        int32_t nsec = int32_t((timeout - sec) * 1e9);
        xt.nsec += nsec;
        if(xt.nsec >= 1000000000)
        {
            ++xt.sec;
            xt.nsec -= 1000000000;
        }
        else if(xt.nsec < 0)
        {
            --xt.sec;
            xt.nsec += 1000000000;
        }
        return xt;
    }


public:
    /// Construct a synchronized queue with a maximum size of \c qMax
    /// items.  The default size is (effectively) unlimited.
    explicit SyncQueue(size_t qMax=size_t(-1)) :
        qMax(qMax),
        nWaiting(0),
        nTasksPending(0),
        cancel(false)
    {}

    /// Destroy queue.  All waiting threads will be cancelled.
    ~SyncQueue() 
    {
        try {
            // Cancel all waiting threads and wait for them to bail
            cancelWaits(true);
        }
        catch(...) {}
    }

    /// Push a new item into the back of the queue, if queue size is
    /// less than the maximum specified in the constructor (unbounded
    /// by default).  If \c wait is set, block until queue has space
    /// available or queue waits are cancelled.  Otherwise, return
    /// immediately.
    /// @return true iff an item was inserted
    bool push(value_t const & x, bool wait=true)
    {
        lock_t l(qMutex);
        for(;;)
        {
            if(q.size() < qMax)
            {
                ++nTasksPending;
                q.push(x);
                itemPushed.notify_one();
                return true;
            }
            if(!wait || cancel)
                return false;

            waitHelper(l, itemPopped);
        }
    }

    /// Push a new item into the back of the queue, if queue size is
    /// less than the maximum specified in the constructor (unbounded
    /// by default).  Wait up to \c timeout seconds for space to
    /// become available, or until queue waits are cancelled.
    /// @return true iff an item was inserted
    bool push(value_t const & x, double timeout)
    {
        if(push(x, false))
            return true;

        boost::xtime xt = getTimeout(timeout);
        lock_t l(qMutex);
        for(;;)
        {
            if(q.size() < qMax)
            {
                ++nTasksPending;
                q.push(x);
                itemPushed.notify_one();
                return true;
            }
            if(cancel || !waitHelper(l, itemPopped, xt))
                return false;
        }
    }

    /// Pop the front item off the queue, if one is available.  If \c
    /// wait is true, block until an item becomes available or queue
    /// waits are cancelled.  Otherwise, return immediately.
    /// @return true iff an item was popped off the queue
    bool pop(value_t & x, bool wait=true)
    {
        lock_t l(qMutex);
        for(;;)
        {
            if(!q.empty())
            {
                x = q.front();
                q.pop();
                itemPopped.notify_one();
                return true;
            }
            if(!wait || cancel)
                return false;

            waitHelper(l, itemPushed);
        }
    }

    /// Pop the front item off the queue, if one is available.  Wait
    /// up to \c timeout seconds for an item to become available, or
    /// until queue waits are cancelled.
    /// @return true iff an item was popped off the queue
    bool pop(value_t & x, double timeout)
    {
        if(pop(x, false))
            return true;

        boost::xtime xt = getTimeout(timeout);
        lock_t l(qMutex);
        for(;;)
        {
            if(!q.empty())
            {
                x = q.front();
                q.pop();
                itemPopped.notify_one();
                return true;
            }
            if(cancel || !waitHelper(l, itemPushed, xt))
                return false;
        }
    }

    void taskComplete()
    {
        lock_t l(qMutex);
        --nTasksPending;
        taskCompleted.notify_one();
    }

    /// Cancel all waits on on this queue.  Calls to push() will
    /// return immediately if the queue is full.  Calls to pop() will
    /// return immediately when the queue is empty.  Helper functions
    /// like waitForEmpty() and waitForCompletion() will return
    /// immediately.  All future waits will also be immediately
    /// cancelled until enableWaits() is called.  If \c wait is set,
    /// block until all other threads cancel.
    void cancelWaits(bool wait=false)
    {
        lock_t l(qMutex);
        for(;;)
        {
            cancel = true;
            itemPushed.notify_all();
            itemPopped.notify_all();
            taskCompleted.notify_all();

            if(!wait || nWaiting == 0)
                break;
            
            zeroWaiting.wait(l);
        }
    }

    /// Wait until queue is empty.  Returns true if the queue is
    /// empty, or false if cancelWaits() was called before that could
    /// happen.
    bool waitForEmpty()
    {
        lock_t l(qMutex);
        for(;;)
        {
            if(q.empty())
                return true;
            if(cancel)
                return false;

            // Wait for something to be popped from the queue.
            waitHelper(l, itemPopped);

            // We're not actually pushing anything, so there's still
            // room for something to be pushed.  Signal the condition
            // again.
            itemPopped.notify_one();
        }
    }

    /// Wait for all items enqueued to be acknowledged with
    /// taskComplete().  The queue consumers must acknowledge the
    /// completion of each item popped using the taskComplete() call.
    /// Returns true if all items have been so acknowledged, or false
    /// if cancelWaits() was called before that could happen.
    bool waitForCompletion()
    {
        lock_t l(qMutex);
        for(;;)
        {
            if(nTasksPending == 0)
                return true;
            if(cancel)
                return false;

            // Wait for a task to be completed.
            waitHelper(l, taskCompleted);
        }
    }

    /// Enable waiting in calls to pop().  Waiting is enabled by
    /// default, but it must be manually re-enabled after a call to
    /// cancelWaits().
    void enableWaits()
    {
        lock_t l(qMutex);
        cancel = false;
    }
    
    /// Returns the size of the current queue.
    /// @return queue size
    size_t size() const
    {
        lock_t l(qMutex);
        return q.size();
    }

    /// Check to see if queue is empty.
    bool empty() const
    {
        lock_t l(qMutex);
        return q.empty();
    }
};

#endif // WARP_SYNCQUEUE_H
