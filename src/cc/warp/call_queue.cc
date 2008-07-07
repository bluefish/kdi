//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-02-10
// 
// This file is part of the warp library.
// 
// The warp library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The warp library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#include <warp/call_queue.h>
#include <ex/exception.h>
#include <boost/bind.hpp>
#include <sys/time.h>
#include <iostream>

using namespace warp;
using namespace ex;
using namespace std;

namespace {

    inline int64_t now()
    {
        struct timeval tv;
        gettimeofday(&tv, 0);
        return int64_t(tv.tv_sec) * 1000000 + int64_t(tv.tv_usec);
    }

    inline boost::xtime getTimeout(int64_t usec)
    {
        boost::xtime xt;
        boost::xtime_get(&xt, boost::TIME_UTC);
        xt.sec += usec / 1000000;
        xt.nsec += (usec % 1000000) * 1000;
        if(xt.nsec >= 1000000000)
        {
            xt.sec += 1;
            xt.nsec -= 1000000000;
        }
        return xt;
    }

}


//----------------------------------------------------------------------------
// CallQueue
//----------------------------------------------------------------------------
void CallQueue::threadLoop()
{
    for(;;)
    {
        // Lock and check for exit condition
        lock_t lock(mutex);
        if(exit)
            break;

        // Call the next item off the heap if it's time
        if(!items.empty() && items.top().when <= now())
        {
            // Copy the top callback and pop it off the heap
            boost::function<void ()> call = items.top().call;
            items.pop();

            // If the heap isn't empty, make sure another thread
            // is awake to handle it, if available
            if(!items.empty())
                wakeup.notify_one();

            // Unlock the queue mutex before invoking the callback
            lock.unlock();

            // Call function -- unhandled exceptions are bad
            try {
                call();
            }
            catch(std::exception const & ex) {
                cerr << "CallQueue: unhandled exception: " << ex.what() << endl;
                ::exit(1);
            }
            catch(...) {
                cerr << "CallQueue: unhandled exception" << endl;
                ::exit(1);
            }

            // Grab queue mutex again -- check exit condition
            // again as it may have changed when we unlocked
            lock.lock();
            if(exit)
                break;
        }

        // Sleep for a while if the queue isn't ready
        if(items.empty())
        {
            // Nothing there -- sleep indefinitely
            wakeup.wait(lock);
        }
        else
        {
            // Wait until the next item is ready
            int64_t dt = items.top().when - now();
            if(dt > 0)
                wakeup.timed_wait(lock, getTimeout(dt));
        }
    }
}

CallQueue::CallQueue(size_t nThreads) :
    exit(false)
{
    if(!nThreads)
        raise<ValueError>("need positive number of call threads");

    for(size_t i = 0; i < nThreads; ++i)
    {
        threads.create_thread(
            boost::bind(&CallQueue::threadLoop, this)
            );
    }
}

CallQueue::~CallQueue()
{
    try {
        lock_t lock(mutex);
        exit = true;
        wakeup.notify_all();
        lock.unlock();
        threads.join_all();
    }
    catch(...) {}
}

void CallQueue::callLater(double delaySeconds,
                          boost::function<void ()> const & call)
{
    Item i;
    i.when = now() + int64_t(delaySeconds * 1e6);
    i.call = call;

    lock_t lock(mutex);
    items.push(i);
    wakeup.notify_one();
}
