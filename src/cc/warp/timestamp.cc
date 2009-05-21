//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-03-21
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
#define _BSD_SOURCE
#include <time.h>
#include <sys/time.h>

#include <warp/timestamp.h>
#include <warp/parsing/timestamp.h>
#include <warp/strutil.h>
#include <ex/exception.h>
#include <limits>

using namespace warp;
using namespace warp::parsing;
using namespace ex;

Timestamp const Timestamp::MAX_TIME = Timestamp::fromMicroseconds(
    std::numeric_limits<int64_t>::max());

Timestamp const Timestamp::MIN_TIME = Timestamp::fromMicroseconds(
    std::numeric_limits<int64_t>::min());

namespace
{
    inline size_t fmt_gmtoff(char * buf, size_t bufsz, int32_t gmtoff,
                             bool extended)
    {
        int s;
        char sign;
        if(gmtoff < 0)
        {
            s = -gmtoff;
            sign = '-';
        }
        else
        {
            s = gmtoff;
            sign = '+';
        }

        int m = s / 60;
        s -= m * 60;
        
        int h = (m / 60) % 24;
        m -= h * 60;

        int sz;
        if(extended)
        {
            if(s)
                sz = snprintf(buf, bufsz, "%c%02d:%02d:%02d", sign, h, m, s);
            else if(m)
                sz = snprintf(buf, bufsz, "%c%02d:%02d", sign, h, m);
            else
                sz = snprintf(buf, bufsz, "%c%02d", sign, h);
        }
        else
        {
            if(m)
                sz = snprintf(buf, bufsz, "%c%02d%02d", sign, h, m);
            else
                sz = snprintf(buf, bufsz, "%c%02d", sign, h);
        }

        if(sz > 0)
            return size_t(sz);
        else
            return 0;
    }
}

Timestamp::Timestamp(strref_t s)
{
    using namespace boost::spirit;
    
    try {
        if(!parse(s.begin(), s.end(),
                  timestamp_p[assign_a(*this)],
                  space_p).full)
        {
            raise<ValueError>("malformed input");
        }
    }
    catch(std::exception const & ex) {
        raise<ValueError>("Timestamp parse failed (%s): %s",
                          ex.what(), reprString(s));
    }
}

void Timestamp::set(int year, int month, int day,
                    int hour, int min, int sec,
                    int32_t usec, int32_t gmtoff)
{
    struct tm t;

    t.tm_sec    = sec;
    t.tm_min    = min;
    t.tm_hour   = hour;
    t.tm_mday   = day;
    t.tm_mon    = month - 1;
    t.tm_year   = year - 1900;
    t.tm_isdst  = 0;

    char tzBuf[64] = "UTC";
    if(gmtoff)
        fmt_gmtoff(tzBuf+3, sizeof(tzBuf)-3, -gmtoff, true);

    // Race condition: mktime() uses the TZ environment variable.  We
    // set and restore here, but we have no way to guarantee another
    // thread won't change it before we manage to call mktime().
    {
        std::string oldTzBuf;
        char const * oldTz = 0;
        if(char const * tz = getenv("TZ"))
        {
            oldTzBuf = tz;
            oldTz = oldTzBuf.c_str();
        }

        setenv("TZ", tzBuf, 1);

        this->usec = mktime(&t);

        if(oldTz)
            setenv("TZ", oldTz, 1);
        else
            unsetenv("TZ");
    }
    // End race

    this->usec  = this->usec * 1000000 + usec;
}

void Timestamp::setLocal(int year, int month, int day,
                         int hour, int min, int sec,
                         int32_t usec)
{
    struct tm t;

    t.tm_sec    = sec;
    t.tm_min    = min;
    t.tm_hour   = hour;
    t.tm_mday   = day;
    t.tm_mon    = month - 1;
    t.tm_year   = year - 1900;
    t.tm_isdst  = -1;

    // Race condition: mktime() uses whatever the current timezone
    // happens to be, which may have changed in another thread since
    // this method was called.
    this->usec  = mktime(&t);
    this->usec  = this->usec * 1000000 + usec;
}

void Timestamp::setUtc(int year, int month, int day,
                       int hour, int min, int sec,
                       int32_t usec)
{
    set(year, month, day,
        hour, min, sec,
        usec, 0);
}


Timestamp Timestamp::now()
{
    struct timeval tv;
    gettimeofday(&tv, 0);

    return Timestamp(int64_t(tv.tv_sec)  * 1000000 +
                     int64_t(tv.tv_usec));
}

std::ostream & warp::operator<<(std::ostream & o, Timestamp const & t)
{
    // Format time as a string
    time_t sec = t.toSeconds();
    int64_t frac = t.toMicroseconds() % 1000000;
    if(frac < 0)
    {
        --sec;
        frac += 1000000;
    }

    struct tm lt;

    // Race condition: localtime_r() uses the last call to tzset(),
    // which uses the TZ environment variable.  If TZ has changed
    // since the last call to tzset(), localtime_r() won't catch that.
    // We always call tzset() to try to get the latest value for the
    // TZ variable.  There's no guarantee another thread won't change
    // it and call tzset() before we manage to call localtime_r().
    {
        tzset();
        localtime_r(&sec, &lt);
    }
    // End race

    if(lt.tm_gmtoff % 60)
    {
        // TZ has a sub-minute offset from GMT, which we can't
        // represent.  Just print as UTC.
        gmtime_r(&sec, &lt);
    }
    
    char buf[256];

    // Write <DATE>T<TIME>.
    {
        size_t sz = strftime(buf, sizeof(buf), "%FT%T.", &lt);
        o.write(buf, sz);
    }

    // Append <MICROSEC>
    {
        int sz = snprintf(buf, sizeof(buf), "%06ld", frac);
        if(sz == 6)
            o.write(buf, sz);
        else
            o.write("000000", 6);
    }
    
    // Append <TZ>
    if(lt.tm_gmtoff == 0)
        o << 'Z';
    else
    {
        char buf[64];
        size_t sz = fmt_gmtoff(buf, sizeof(buf), lt.tm_gmtoff, false);
        o.write(buf, sz);
    }

    return o;
}
