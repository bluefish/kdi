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

#include <warp/perflog.h>
#include <warp/md5.h>
#include <warp/strutil.h>
#include <ex/exception.h>
#include <boost/bind.hpp>
#include <algorithm>
#include <sys/time.h>
#include <stdio.h>
#include <unistd.h>

using namespace warp;
using namespace ex;
using namespace std;

namespace
{
    PerformanceInfo makePInfo(int64_t flags=0)
    {
        PerformanceInfo info;
        info.currentTimeNs = PerformanceLog::getCurrentNs();
        info.deltaTimeNs = 0;
        info.flags = flags;
        return info;
    }

    struct AutoBool
    {
        bool & var;

        explicit AutoBool(bool & var) : var(var)
        {
            var = true;
        }
        ~AutoBool()
        {
            var = false;
        }
    };
}

//----------------------------------------------------------------------------
// PerformanceLog
//----------------------------------------------------------------------------
void PerformanceLog::generateId()
{
    // Get some components that should uniquely identify this log instance
    char hostname[512];
    if(0 != gethostname(hostname, sizeof(hostname)))
        hostname[0] = 0;
    pid_t pid = getpid();
    uintptr_t here = reinterpret_cast<uintptr_t>(this);
    time_t now = time(0);

    // Compute a hash of our identifiers
    Md5 md5;
    md5.update(hostname, strlen(hostname));
    md5.update(&pid, sizeof(pid));
    md5.update(&here, sizeof(here));
    md5.update(&now, sizeof(now));

    // Get digest as 8 shorts
    uint16_t dig[8];
    md5.digest(dig);

    // Map shorts to 8 printable characters
    char const * alpha = "123456789"    // omit 0
        "qwertyuiopasdfghjkzxcvbnm"     // omit l
        "QWERTYUIPASDFGHJKLZXCVBNM";    // omit O
    size_t sz = strlen(alpha);          // should be 59
    logId.clear();
    for(size_t i = 0; i < 8; ++i)
        logId += alpha[dig[i] % sz];
}

void PerformanceLog::addUnsync(PerformanceItem * x)
{
    if(inMonitor)
        raise<RuntimeError>("cannot add performance item from log callback");

    if(x->tracker)
    {
        if(x->tracker == this)
            return;

        x->tracker->removeItem(x);
    }

    // Do EVENT_FIRST callback
    if(loggingEnabled)
    {
        PerformanceInfo info = makePInfo(PerformanceInfo::EVENT_FIRST);
        string msg;
        if(x->getMessage(msg, info))
            logUnsync(info.currentTimeNs, x->getType(), msg.c_str());
    }

    // Add to tracked set
    x->tracker = this;
    x->trackerIndex = items.size();
    items.push_back(x);
}

void PerformanceLog::removeUnsync(PerformanceItem * x)
{
    if(inMonitor)
        raise<RuntimeError>("cannot remove performance item from log callback");

    if(x->tracker != this)
        return;

    if(loggingEnabled && !x->inTeardown)
    {
        // Do EVENT_LAST callback
        PerformanceInfo info = makePInfo(PerformanceInfo::EVENT_LAST);
        string msg;
        if(x->getMessage(msg, info))
            logUnsync(info.currentTimeNs, x->getType(), msg.c_str());
    }

    // Remove from tracked set
    std::swap(items[x->trackerIndex], items.back());
    items[x->trackerIndex]->trackerIndex = x->trackerIndex;
    items.pop_back();
    x->tracker = 0;
}

void PerformanceLog::logUnsync(int64_t t, char const * type, char const * msg)
{
    if(!loggingEnabled)
        return;

    char buf[4096];
    size_t n = snprintf(buf, sizeof(buf), "%ld %s %s %s\n",
                        t, logId.c_str(), type, msg);

    // Only write to log from logging thread.  Otherwise add
    // message to pending buffer.
    if(inMonitor)
    {
        logFile->write(buf, n);
        shouldFlush = true;
    }
    else
    {
        pending.push_back(string(buf, buf+n));
    }
}

void PerformanceLog::threadLoop()
{
    int64_t lastUpdateNs;

    // Do EVENT_FIRST callbacks
    {
        lock_t l(logMutex);
        AutoBool monitorSection(inMonitor);

        PerformanceInfo info = makePInfo(PerformanceInfo::EVENT_FIRST);
        lastUpdateNs = info.currentTimeNs;
        string msg;
        for(vector<PerformanceItem *>::const_iterator t = items.begin();
            t != items.end(); ++t)
        {
            if((*t)->getMessage(msg, info))
                logUnsync(info.currentTimeNs, (*t)->getType(), msg.c_str());
        }
    }

    // Loop until someone tells us to exit
    bool keepGoing = true;
    while(keepGoing)
    {
        // Wait for 5 seconds or termination condition
        boost::xtime xt;
        boost::xtime_get(&xt, boost::TIME_UTC);
        xt.sec += 5;
        
        // Get log lock
        lock_t l(logMutex);
        
        // Check for exit and sleep for a while if it's not time yet
        if(threadExit || exitCondition.timed_wait(l, xt) || threadExit)
        {
            // Exit condition met -- do final cleanup round
            keepGoing = false;
        }

        // Mark that we're in the monitor thread -- prevents
        // addItem() and removeItem(), and causes calls to log()
        // to go directly to output file.
        AutoBool monitorSection(inMonitor);

        // Only flush if we write something out this update
        shouldFlush = false;

        // Dump any pending messages that may have accumulated
        for(vector<string>::const_iterator p = pending.begin();
            p != pending.end(); ++p)
        {
            logFile->write(p->c_str(), p->size());
            shouldFlush = true;
        }
        pending.clear();

        // Prepare PerformanceInfo for callbacks
        PerformanceInfo info;
        info.currentTimeNs = getCurrentNs();
        info.deltaTimeNs = info.currentTimeNs - lastUpdateNs;
        info.flags = keepGoing ? 0 : PerformanceInfo::EVENT_LAST;

        // Do tracked item log callbacks
        string msg;
        for(vector<PerformanceItem *>::const_iterator t = items.begin();
            t != items.end(); ++t)
        {
            if((*t)->getMessage(msg, info))
                logUnsync(info.currentTimeNs, (*t)->getType(), msg.c_str());
        }

        // If something got written, flush
        if(shouldFlush)
            logFile->flush();

        // Note last update time for delta tracking
        lastUpdateNs = info.currentTimeNs;
    }
}

PerformanceLog::PerformanceLog() :
    inMonitor(false), shouldFlush(false), loggingEnabled(false), threadExit(false)
{
    generateId();
}

PerformanceLog::~PerformanceLog()
{
    try {
        // Remove all items
        {
            lock_t l(logMutex);
            while(!items.empty())
                removeUnsync(items.back());
        }

        // Stop logging
        if(loggingEnabled)
            stopLogging();
    }
    catch(...) {
        // No exceptions!
    }
}

void PerformanceLog::addItem(PerformanceItem * x)
{
    lock_t l(logMutex);
    addUnsync(x);
}

void PerformanceLog::removeItem(PerformanceItem * x)
{
    lock_t l(logMutex);
    removeUnsync(x);
}

void PerformanceLog::log(string const & type, string const & msg)
{
    if(!loggingEnabled)
        return;

    lock_t l(logMutex);
    logUnsync(getCurrentNs(), type.c_str(), msg.c_str());
}

void PerformanceLog::startLogging(string const & outFn)
{
    if(loggingEnabled)
        raise<RuntimeError>("logging already enabled");

    // Set logging flag
    loggingEnabled = true;

    // Call start hooks
    for(vector<hook_t>::const_iterator h = startHooks.begin();
        h != startHooks.end(); ++h)
    {
        (**h)(this);
    }


    // Open log file
    logFile = File::append(outFn);

    // Clear thread exit flag
    {
        lock_t l(logMutex);
        threadExit = false;
    }

    // Start thread
    using boost::bind;
    threadPtr.reset(new thread_t(bind(&PerformanceLog::threadLoop, this)));
}

void PerformanceLog::stopLogging()
{
    if(!loggingEnabled)
        raise<RuntimeError>("logging has not been started");

    // Call stop hooks
    for(vector<hook_t>::const_iterator h = stopHooks.end();
        h != stopHooks.begin(); /**/)
    {
        (**--h)(this);
    }

    // Set thread exit flag
    {
        lock_t l(logMutex);
        threadExit = true;
    }

    // Stop logging thread
    if(threadPtr)
    {
        exitCondition.notify_one();
        threadPtr->join();
        threadPtr.reset();
    }

    // Close log file
    logFile.reset();

    // Clear logging flag
    loggingEnabled = false;
}

void PerformanceLog::addHooks(hook_t startHook, hook_t stopHook)
{
    if(startHook)
    {
        // Call hook immediately if we're already started
        if(loggingEnabled)
            (*startHook)(this);

        startHooks.push_back(startHook);
    }
    
    if(stopHook)
        stopHooks.push_back(stopHook);
}

bool PerformanceLog::isLogging() const
{
    return loggingEnabled;
}

int64_t PerformanceLog::getCurrentNs()
{
    struct timeval t;
    gettimeofday(&t, 0);
    return int64_t(t.tv_sec) * 1000000000 + t.tv_usec * 1000;

    // struct timespec t;
    // clock_gettime(CLOCK_MONOTONIC, &t);
    // return int64_t(t.tv_sec) * 1000000000 + t.tv_nsec;
}

PerformanceLog & PerformanceLog::getCommon()
{
    static PerformanceLog commonLog;
    return commonLog;
}



//----------------------------------------------------------------------------
// Init
//----------------------------------------------------------------------------
#include <warp/init.h>
#include <stdlib.h>

WARP_DEFINE_INIT(warp_perflog)
{
    if(char const * fn = getenv("KOSMIX_PERFLOG"))
        PerformanceLog::getCommon().startLogging(fn);
}
