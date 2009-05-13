//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-13
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

#include <warp/PersistentWorker.h>
#include <warp/log.h>
#include <ex/exception.h>
#include <boost/bind.hpp>

using namespace warp;

//----------------------------------------------------------------------------
// PersistentWorker
//----------------------------------------------------------------------------
PersistentWorker::PersistentWorker(std::string const & name) :
    name(name), workerActive(false), shouldRun(false)
{
}

PersistentWorker::~PersistentWorker()
{
    log("%s worker: cleanup", name);

    lock_t lock(mutex);
    shouldRun = false;
    workCond.notify_all();
    while(workerActive)
        exitCond.wait(lock);
    thread.reset();
    
    log("%s worker: cleanup complete", name);
}

void PersistentWorker::start()
{
    log("%s worker: start", name);

    lock_t lock(mutex);
    if(shouldRun)
        return;

    while(workerActive)
        exitCond.wait(lock);

    shouldRun = true;
    thread.reset(
        new boost::thread(
            boost::bind(
                &PersistentWorker::workerLoop,
                this)
            )
        );
}

void PersistentWorker::stop()
{
    log("%s worker: stop", name);

    lock_t lock(mutex);
    shouldRun = false;
    workCond.notify_all();
}

void PersistentWorker::wait() const
{
    log("%s worker: wait", name);

    lock_t lock(mutex);
    while(workerActive)
        exitCond.wait(lock);

    log("%s worker: wait complete", name);
}

void PersistentWorker::wake()
{
    log("%s worker: wake", name);

    lock_t lock(mutex);
    workCond.notify_all();
}

void PersistentWorker::workerLoop()
{
    log("%s worker: starting", name);

    bool waitForWork = false;
    lock_t lock(mutex);
    workerActive = true;
    while(shouldRun)
    {
        if(waitForWork)
        {
            workCond.wait(lock);
            waitForWork = false;
            continue;
        }

        lock.unlock();
        try {
            waitForWork = !doWork();
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
        lock.lock();
    }
    workerActive = false;
    exitCond.notify_all();

    log("%s worker: exiting", name);
}

bool PersistentWorker::isCancelled() const
{
    lock_t lock(mutex);
    return !shouldRun;
}
