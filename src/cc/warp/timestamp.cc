//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/timestamp.cc $
//
// Created 2008/03/21
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
