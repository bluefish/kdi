//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-28
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

#ifndef WARP_QUEUEDWORKER_H
#define WARP_QUEUEDWORKER_H

#include <warp/syncqueue.h>
#include <warp/log.h>
#include <ex/exception.h>
#include <string>
#include <exception>
#include <boost/noncopyable.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>

namespace warp {

    template <class Work>
    class QueuedWorker;

} // namespace warp

//----------------------------------------------------------------------------
// QueuedWorker
//----------------------------------------------------------------------------
template <class Work>
class warp::QueuedWorker
    : private boost::noncopyable
{
public:
    typedef Work work_t;
    typedef Work const & work_cref_t;

public:
    explicit QueuedWorker(std::string const & name) :
        name(name)
    {
    }

    ~QueuedWorker()
    {
        log("%s worker: cleanup", name);

        workQ.cancelWaits();
        boost::mutex::scoped_lock lock(mutex);
        if(thread)
        {
            thread->join();
            thread.reset();
        }

        log("%s worker: cleanup complete", name);
    }

    /// Start the background worker.  If the worker has already been
    /// started, do nothing.
    void start()
    {
        log("%s worker: start", name);

        boost::mutex::scoped_lock lock(mutex);
        if(thread)
            return;

        thread.reset(
            new boost::thread(
                boost::bind(
                    &QueuedWorker<work_t>::workerLoop, this)));
    }

    /// Queue more work for background processing.
    void submit(work_cref_t x)
    {
        workQ.push(x);
    }

protected:
    virtual void processWork(work_cref_t x) = 0;

private:
    void workerLoop()
    {
        log("%s worker: starting", name);

        for(;;)
        {
            work_t x;
            if(!workQ.pop(x))
                break;

            try {
                processWork(x);
            }
            catch(ex::Exception const & ex) {
                log("%s worker error: %s", name, ex);
                std::terminate();
            }
            catch(std::exception const & ex) {
                log("%s worker error: %s", name, ex.what());
                std::terminate();
            }
            catch(...) {
                log("%s worker error: unknown exception", name);
                std::terminate();
            }
        }

        log("%s worker: exiting", name);
    }

private:
    SyncQueue<work_t> workQ;
    boost::scoped_ptr<boost::thread> thread;
    std::string name;
    boost::mutex mutex;
};

#endif // WARP_QUEUEDWORKER_H
