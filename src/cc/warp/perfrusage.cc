//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-01-22
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
