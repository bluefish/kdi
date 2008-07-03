//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-01-15
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

#ifndef WARP_PERFLOG_H
#define WARP_PERFLOG_H

#include "perfitem.h"
#include "file.h"
#include <string>
#include <vector>
#include <boost/utility.hpp>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <stdint.h>
#include <stddef.h>

namespace warp
{
    class PerformanceLog;
}


//----------------------------------------------------------------------------
// PerformanceLog
//----------------------------------------------------------------------------
class warp::PerformanceLog : private boost::noncopyable
{
    typedef boost::recursive_mutex mutex_t;
    typedef mutex_t::scoped_lock lock_t;
    typedef boost::thread thread_t;
    typedef boost::condition condition_t;
    typedef void (*hook_t)(PerformanceLog *);

    // Shared and synchronized
    std::vector<std::string> pending;
    std::vector<PerformanceItem *> items;

    // Only accessed by monitor thread
    FilePtr logFile;
    bool inMonitor;
    bool shouldFlush;

    // Only accessed by main thread
    std::vector<hook_t> startHooks;
    std::vector<hook_t> stopHooks;
    boost::shared_ptr<thread_t> threadPtr;
    std::string logId;
    bool loggingEnabled;
    bool threadExit;

    // Synchronization
    mutex_t logMutex;
    condition_t exitCondition;

private:
    /// Generate a (probabilistically) unique string for this log
    /// instance.  Takes into account machine host name, process
    /// number, time of day, and log memory address to produce an 8
    /// character hash.  The ID is stored internally and emitted in
    /// all messages from this log.
    void generateId();

    /// Add a tracked item to the log.  Unsynchronized.  Cannot be
    /// called by the logging thread.
    void addUnsync(PerformanceItem * x);

    /// Remove a tracked item from the log.  Unsynchronized.  Cannot
    /// be called by the logging thread.
    void removeUnsync(PerformanceItem * x);

    /// Write a log message.  Unsynchronized.  If called by the
    /// logging thread, write directly to output file.  Otherwise,
    /// buffer message for consumption by logging thread.
    void logUnsync(int64_t t, char const * type, char const * msg);

    /// Loop for logging thread.  Periodically does callbacks for
    /// tracked items and flushes log output file.
    void threadLoop();

public:
    PerformanceLog();
    ~PerformanceLog();

    /// Add an item to be monitored by the log.  An item may only be a
    /// member of one log at a time.  If it is already associated with
    /// a different log, it will be removed before adding it to this
    /// one.
    void addItem(PerformanceItem * x);

    /// Remove an item from this log.  If the item is not associated
    /// with this log, do nothing.
    void removeItem(PerformanceItem * x);

    /// Log a performance message.  If the log has not been started,
    /// ignore the message.  This function may block while the logging
    /// thread is writing.
    void log(std::string const & type, std::string const & msg);

    /// Initialize performance logging facility.  Output will be
    /// directed to the named file.  All start hooks that have been
    /// previously registered will be called.
    void startLogging(std::string const & outFn);

    /// Terminate logging thread and close log file.
    void stopLogging();

    /// Add hooks to be called when logging is started or stopped.  If
    /// logging has already been started, the start hook is called
    /// immediately.  The hooks will be called every time logging is
    /// started or stopped, which could happen several times.  The
    /// hook type is a function pointer accepting a PerformanceLog
    /// pointer as an argument and returning void.  Either hook may be
    /// null, in which case it will not be called.  Stop hooks are
    /// called in reverse order of start hooks.
    void addHooks(hook_t startHook, hook_t stopHook);

    /// Return true if performance logging has been started.
    bool isLogging() const;

    /// Get the current timestamp in nanoseconds.
    static int64_t getCurrentNs();

    /// Get common shared performance log.
    static PerformanceLog & getCommon();
};


#endif // WARP_PERFLOG_H
