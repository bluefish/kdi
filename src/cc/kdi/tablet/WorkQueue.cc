//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/WorkQueue.cc $
//
// Created 2008/08/19
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
}

WorkQueue::~WorkQueue()
{
    shutdown();
}

void WorkQueue::post(job_t const & job)
{
    jobs.push(job);
}

void WorkQueue::shutdown()
{
    jobs.cancelWaits();
    threads.join_all();
}

void WorkQueue::workLoop()
{
    try {
        job_t job;
        while(jobs.pop(job))
            job();
    }
    catch(std::exception const & ex) {
        log("WorkQueue error: %s", ex.what());
        ::exit(1);
    }
    catch(...) {
        log("WorkQueue error: unknown exception");
        ::exit(1);
    }
}
