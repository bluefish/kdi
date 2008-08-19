//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/WorkQueue.h $
//
// Created 2008/08/19
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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

public:
    explicit WorkQueue(size_t nThreads);
    ~WorkQueue();

    void post(job_t const & job);
    void shutdown();

private:
    void workLoop();
};

#endif // KDI_TABLET_WORKQUEUE_H
