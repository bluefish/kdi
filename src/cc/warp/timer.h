//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/timer.h#1 $
//
// Created 2006/05/12
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_TIMER_H
#define WARP_TIMER_H

#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>
#include <stdint.h>
#include <boost/noncopyable.hpp>

namespace warp
{
    class TimeInterval;

    template <class T>
    class AutoInterval;

    class WallTimer;
    class CpuTimer;
}


//----------------------------------------------------------------------------
// TimeInterval
//----------------------------------------------------------------------------
class warp::TimeInterval
{
    int64_t t0;
    int64_t t1;

public:
    TimeInterval() : t0(0), t1(0) {}

    void setStartTimeNs(int64_t nanoSeconds) { t0 = nanoSeconds; }
    void setStopTimeNs(int64_t nanoSeconds) { t1 = nanoSeconds; }

    int64_t getStartTimeNs() const { return t0; }
    int64_t getStopTimeNs() const { return t1; }

    int64_t getElapsedNs() const { return t1 - t0; }
    double getElapsed() const { return getElapsedNs() * 1e-9; }
};


//----------------------------------------------------------------------------
// AutoInterval
//----------------------------------------------------------------------------
template <class T>
class warp::AutoInterval : private boost::noncopyable
{
    T const & timer;
    TimeInterval & interval;

public:
    AutoInterval(T const & timer, TimeInterval & interval) :
        timer(timer), interval(interval)
    {
        interval.setStartTimeNs(timer.getElapsedNs());
    }

    ~AutoInterval()
    {
        interval.setStopTimeNs(timer.getElapsedNs());
    }
};


//----------------------------------------------------------------------------
// WallTimer
//----------------------------------------------------------------------------
class warp::WallTimer
{
    typedef struct timeval tv_t;

    tv_t t0;

public:
    WallTimer() { reset(); }

    void reset() { gettimeofday(&t0, 0); }

    int64_t getElapsedNs() const
    {
        tv_t t1;
        gettimeofday(&t1, 0);

        int64_t dt = t1.tv_sec - t0.tv_sec;
        dt *= 1000000;
        dt += t1.tv_usec - t0.tv_usec;
        dt *= 1000;
        return dt;
    }

    double getElapsed() const
    {
        return getElapsedNs() * 1e-9;
    }
};


//----------------------------------------------------------------------------
// CpuTimer
//----------------------------------------------------------------------------
class warp::CpuTimer
{
    typedef struct rusage ru_t;

    ru_t r0;

public:
    CpuTimer() { reset(); }

    void reset() { getrusage(RUSAGE_SELF, &r0); }

    int64_t getElapsedNs() const
    {
        ru_t r1;
        getrusage(RUSAGE_SELF, &r1);

        int64_t dt = r1.ru_utime.tv_sec + r1.ru_stime.tv_sec -
            r0.ru_utime.tv_sec - r0.ru_stime.tv_sec;
        dt *= 1000000;
        dt += r1.ru_utime.tv_usec + r1.ru_stime.tv_usec -
            r0.ru_utime.tv_usec - r0.ru_stime.tv_usec;
        dt *= 1000;
        return dt;
    }
    double getElapsed() const
    {
        return getElapsedNs() * 1e-9;
    }
};

#endif // WARP_TIMER_H
