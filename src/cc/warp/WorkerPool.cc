//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-10
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

#include <warp/WorkerPool.h>
#include <warp/call_or_die.h>
#include <sstream>
#include <boost/bind.hpp>

using namespace warp;

void WorkerPool::workerLoop()
{
    Runnable * task = 0;
    while(tasks.pop(task))
        task->run();
}

WorkerPool::WorkerPool(size_t nWorkers, std::string const & poolName,
                       bool verbose)
{
    for(size_t i = 0; i < nWorkers; ++i)
    {
        std::ostringstream oss;
        oss << poolName << "-" << i;
        workers.create_thread(
            warp::callOrDie(
                boost::bind(
                    &WorkerPool::workerLoop,
                    this),
                oss.str(), verbose));
    }
}

WorkerPool::~WorkerPool()
{
    tasks.cancelWaits();
    workers.join_all();
}
