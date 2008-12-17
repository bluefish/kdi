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

#include <kdi/tablet/WorkQueue.h>
#include <warp/log.h>
#include <warp/call_or_die.h>
#include <ex/exception.h>
#include <boost/bind.hpp>

using namespace kdi::tablet;
using namespace warp;
using namespace ex;

WorkQueue::WorkQueue(size_t nThreads) :
    done(false)
{
    if(!nThreads)
        raise<ValueError>("WorkQueue needs at least 1 thread");

    for(size_t i = 0; i < nThreads; ++i)
    {
        threads.create_thread(
            callOrDie(
                boost::bind(&WorkQueue::workLoop, this),
                "Work thread", true
                )
            );
    }

    log("WorkQueue %p: created, %d thread(s)", this, nThreads);
}

WorkQueue::~WorkQueue()
{
    if(!done)
        shutdown();

    log("WorkQueue %p: destroyed", this);
}

void WorkQueue::post(job_t const & job)
{
    jobs.push(job);
}

void WorkQueue::shutdown()
{
    if(done)
        raise<RuntimeError>("WorkQueue already shut down");

    log("WorkQueue shutdown");
    jobs.cancelWaits();
    threads.join_all();
    done = true;
}

void WorkQueue::workLoop()
{
    for(;;)
    {
        // This loop is structured so that a new job instance is used
        // each time.  It turns out that reusing the same job each
        // time can cause deadlock: WorkQueue pops jobs from a
        // SyncQueue.  The pop method assigns the popped element to
        // the passed in reference while holding a the queue lock.
        // The assignment calls the destructor of previously assigned
        // bound functor.  The bound functor holds a shared pointer to
        // a Tablet.  The tablet has been released by everything else,
        // and this triggers the tablet's destructor.  The destructor
        // tries to get the dagMutex so it can remove the Tablet from
        // the FragDag.  Unfortunately, it has to wait because the
        // FragDag is currently replacing fragments in another Tablet.
        // That tablet has replaced the fragments and needs to queue
        // up a config change.  To do that, it tries to push a job
        // into the WorkQueue, which is currently blocked in the pop
        // method.  Ouch.

        job_t job;
        if(!jobs.pop(job))
            break;
        job();
    }
}
