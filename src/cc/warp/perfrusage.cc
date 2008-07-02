//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/perfrusage.cc#1 $
//
// Created 2007/01/22
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "perfvar.h"
#include "perflog.h"
#include "strutil.h"
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>

using namespace warp;

namespace
{
    class UserCpuTime : public PerformanceInteger
    {
    public:
        UserCpuTime() : PerformanceInteger("cputime user (nsec)") {}
        virtual bool getMessage(std::string & msg, PerformanceInfo const & info)
        {
            rusage ru;
            if(0 == getrusage(RUSAGE_SELF, &ru))
            {
                setValue(int64_t(ru.ru_utime.tv_sec) * 1000000000 +
                         ru.ru_utime.tv_usec * 1000);
            }
            return PerformanceInteger::getMessage(msg, info);
        }
    };

    class SystemCpuTime : public PerformanceInteger
    {
    public:
        SystemCpuTime() : PerformanceInteger("cputime system (nsec)") {}
        virtual bool getMessage(std::string & msg, PerformanceInfo const & info)
        {
            rusage ru;
            if(0 == getrusage(RUSAGE_SELF, &ru))
            {
                setValue(int64_t(ru.ru_stime.tv_sec) * 1000000000 +
                         ru.ru_stime.tv_usec * 1000);
            }
            return PerformanceInteger::getMessage(msg, info);
        }
    };

    class VSizeMonitor : public PerformanceInteger
    {
        int pageSize;

    public:
        VSizeMonitor() : PerformanceInteger("memory vsize (bytes)")
        {
            pageSize = getpagesize();
        }

        virtual bool getMessage(std::string & msg, PerformanceInfo const & info)
        {
            char buf[256];
            int fd = open("/proc/self/statm", O_RDONLY);
            if(fd != -1)
            {
                ssize_t rd = read(fd, buf, sizeof(buf));
                close(fd);

                if(rd > 0)
                {
                    // Get first field
                    char const * end = buf + rd;
                    char const * p0 = skipSpace((char const *)buf, end);
                    char const * p1 = skipNonSpace(p0, end);
                    
                    int64_t x;
                    if(parseInt(x, p0, p1))
                        setValue(x * pageSize);
                }
            }

            return PerformanceInteger::getMessage(msg, info);
        }
    };

    class RssMonitor : public PerformanceInteger
    {
        int pageSize;

    public:
        RssMonitor() : PerformanceInteger("memory rss (bytes)")
        {
            pageSize = getpagesize();
        }

        virtual bool getMessage(std::string & msg, PerformanceInfo const & info)
        {
            char buf[256];
            int fd = open("/proc/self/statm", O_RDONLY);
            if(fd != -1)
            {
                ssize_t rd = read(fd, buf, sizeof(buf));
                close(fd);

                if(rd > 0)
                {
                    // Get second field
                    char const * end = buf + rd;
                    char const * p0 = skipSpace((char const *)buf, end);
                    p0 = skipNonSpace(p0, end);
                    p0 = skipSpace(p0, end);
                    char const * p1 = skipNonSpace(p0, end);
                    
                    int64_t x;
                    if(parseInt(x, p0, p1))
                        setValue(x * pageSize);
                }
            }

            return PerformanceInteger::getMessage(msg, info);
        }
    };
}


#include "init.h"
WARP_DEFINE_INIT(warp_perfrusage)
{
    PerformanceLog & log = PerformanceLog::getCommon();

    static UserCpuTime utime;
    static SystemCpuTime stime;
    static VSizeMonitor vsize;
    static RssMonitor rss;

    log.addItem(&utime);
    log.addItem(&stime);
    log.addItem(&vsize);
    log.addItem(&rss);
}
