//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-03-21
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

#include <warp/timestamp.h>
#include <warp/parsing/timestamp.h>
#include <ex/exception.h>
#include <sys/time.h>

using namespace warp;
using namespace warp::parsing;
using namespace ex;

Timestamp::Timestamp(strref_t s)
{
    using namespace boost::spirit;
    
    try {
        if(!parse(s.begin(), s.end(),
                  timestamp_p[assign_a(usec)],
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
    time_t sec = t / 1000000;
    int64_t frac = t % 1000000;
    struct tm gmt;
    gmtime_r(&sec, &gmt);
    char buf[256];
    {
        size_t sz = strftime(buf, sizeof(buf), "%FT%T.", &gmt);
        o.write(buf, sz);
    }
    {
        int sz = snprintf(buf, sizeof(buf), "%06ldZ", frac);
        if(sz == 7)
            o.write(buf, sz);
        else
            o.write("000000Z", 7);
    }

    return o;
}
