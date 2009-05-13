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

#ifndef WARP_PERSISTENTWORKER_H
#define WARP_PERSISTENTWORKER_H

#include <boost/noncopyable.hpp>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <string>

namespace warp {

    class PersistentWorker;

} // namespace warp

//----------------------------------------------------------------------------
// PersistentWorker
//----------------------------------------------------------------------------
class warp::PersistentWorker
    : private boost::noncopyable
{
public:
    /// Initialize the worker.  The background thread will not be
    /// started until someone calls start().
    explicit PersistentWorker(std::string const & name);

    /// Cleanup.  The destructor will stop() and wait().
    virtual ~PersistentWorker();

    /// Start the background worker.  If the worker has already been
    /// started, do nothing.
    void start();

    /// Signal that background worker should exit at its earliest
    /// convenience.  This function returns immediately.  To wait for
    /// the worker to exit, use wait().
    void stop();

    /// Wait for the background worker to exit.  This function doesn't
    /// signal an exit, so be sure to call stop() before waiting.
    void wait() const;

    /// Signal that there is more work available.
    void wake();

protected:
    /// Derived classes should implement this function.  The function
    /// should do some incremental amount of work and return true, or
    /// return false if there is no more work to do at the moment.
    virtual bool doWork() = 0;

    /// Returns true if the worker has been stopped.  The doWork()
    /// function can check this in case it wants to abort in
    /// particularly long chunk of work.
    bool isCancelled() const;

private:
    /// Internal loop for worker thread
    void workerLoop();
    
private:
    typedef boost::mutex::scoped_lock lock_t;

private:
    mutable boost::mutex mutex;
    mutable boost::condition workCond;
    mutable boost::condition exitCond;

    boost::scoped_ptr<boost::thread> thread;
    std::string name;
    bool workerActive;
    bool shouldRun;
};

#endif // WARP_PERSISTENTWORKER_H
