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
#include <ex/exception.h>
#include <boost/bind.hpp>

using namespace kdi::tablet;
using namespace warp;
using namespace ex;

WorkQueue::WorkQueue(size_t nThreads)
{
    if(!nThreads)
        raise<ValueError>("WorkQueue needs at least 1 thread");

    for(size_t i = 0; i < nThreads; ++i)
    {
        threads.create_thread(
            boost::bind(&WorkQueue::workLoop, this)
            );
    }

    log("WorkQueue %p: created, %d thread(s)", this, nThreads);
}

WorkQueue::~WorkQueue()
{
    shutdown();

    log("WorkQueue %p: destroyed", this);
}

void WorkQueue::post(job_t const & job)
{
    //log("WorkQueue push");
    jobs.push(job);
}

void WorkQueue::shutdown()
{
    log("WorkQueue shutdown");
    jobs.cancelWaits();
    threads.join_all();
}

void WorkQueue::workLoop()
{
    log("Work thread starting");
    try {
        job_t job;
        while(jobs.pop(job))
        {
            //log("WorkQueue pop");
            job();
        }
    }
    catch(std::exception const & ex) {
        log("WorkQueue error: %s", ex.what());
        ::exit(1);
    }
    catch(...) {
        log("WorkQueue error: unknown exception");
        ::exit(1);
    }
    log("Work thread exiting");
}
