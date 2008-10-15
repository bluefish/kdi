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

#ifndef WARP_PARSING_TIMESTAMP_H
#define WARP_PARSING_TIMESTAMP_H

#include <warp/parsing/numeric.h>
#include <time.h>
#include <string.h>
#include <boost/spirit.hpp>

namespace warp {
namespace parsing {

    /// Parse a timestamp and convert it into an integer
    /// representing the number of microseconds since
    /// 1970-01-01T00:00:00Z.  The timestamp may either be an ISO8601
    /// date time string or an integer literal prefixed by an '@'.
    class TimestampParser
    {
        int localTimezone;
        int isDst;

    public:
        typedef int64_t result_t;
    
        TimestampParser()
        {
            // Get timezone information from globals in <time.h>
            //  'timezone' is the difference in seconds between UTC and
            //  local time.  It is only defined after calling tzset().
            //  There's also a 'daylight' variable, but it only says
            //  whether the local timezone has a daylight savings, not
            //  whether we're currently in it.  Bah.
            //  For whatever reason, the 'timezone' variable is opposite
            //  the usual convention of being the difference between local
            //  and UTC.  We want the normal convention, so we invert it
            //  here.
            tzset();
            localTimezone = -timezone;

            // Get local daylight savings setting
            time_t currentTime = time(0);
            struct tm currentTm;
            localtime_r(&currentTime, &currentTm);
            isDst = currentTm.tm_isdst;
        }

        template <typename ScannerT>
        std::ptrdiff_t
        operator()(ScannerT const & scan, result_t & result) const
        {
            using namespace boost::spirit;

            struct tm t;
            memset(&t, 0, sizeof(t));
            t.tm_mday = 1;
            t.tm_isdst = isDst;
            uint64_t usec = 0;
    
            int parsedTimezone = localTimezone;
    
            bool tzIsNegative = false;
            int tzHour = 0;
            int tzMin = 0;
    
            return
                (
                    ( // Integer literal
                        ch_p('@') >> int64_p[assign_a(result)]
                    ) |
                    ( // ISO8601 date time
                        // Year
                        uint4_p[assign_a(t.tm_year)] >> 
                        // Optional month
                        !( !ch_p('-') >> uint2_p[assign_a(t.tm_mon)] >>
                           // Optional day
                           !( !ch_p('-') >> uint2_p[assign_a(t.tm_mday)] )
                         ) >>
                        // Optional time value
                        !( (ch_p('T') | ch_p('t')) >>
                           // Hour
                           uint2_p[assign_a(t.tm_hour)] >>
                           // Optional minute
                           !( !ch_p(':') >> uint2_p[assign_a(t.tm_min)] >>
                              // Optional second
                              !( !ch_p(':') >> uint2_p[assign_a(t.tm_sec)] >>
                                 // Optional microsecond
                                 !( ch_p('.') >> (+digit_p)[AssignMicroseconds(usec)] )
                               )
                            )
                         ) >>
                        // Optional time zone
                        !( (ch_p('Z') | ch_p('z'))[assign_a(parsedTimezone, 0)][assign_a(t.tm_isdst, 0)] |
                           ( sign_p[assign_a(tzIsNegative)] >> 
                             uint2_p[assign_a(tzHour)] >>
                             !( !ch_p(':') >> uint2_p[assign_a(tzMin)] )
                           )[AssignTimezone(parsedTimezone, tzIsNegative, tzHour, tzMin)][assign_a(t.tm_isdst, 0)]
                         )
                    )[AssignDate(result, t, usec, parsedTimezone, localTimezone)]
                )
                .parse(scan)
                .length();
        }
    
    private:
        struct AssignMicroseconds
        {
            uint64_t & usec;

            AssignMicroseconds(uint64_t & usec) : usec(usec) {}
            
            template <class Iter>
            void operator()(Iter a, Iter b) const
            {
                int digits = 0;
                Iter end = a;

                // Select up to 6 digits for the microsecond field
                for(; end != b && digits < 6; ++end, ++digits)
                    ;

                // Parse the digits
                parseInt(usec, a, end);

                // Extend so we have at least six digits
                for(; digits < 6; ++digits)
                    usec *= 10;
            }
        };

        struct AssignTimezone
        {
            int & tzOffset;
            bool const & tzIsNegative;
            int const & tzHour;
            int const & tzMin;
    
            AssignTimezone(int & tzOffset,
                           bool const & tzIsNegative,
                           int const & tzHour,
                           int const & tzMin) :
                tzOffset(tzOffset), tzIsNegative(tzIsNegative),
                tzHour(tzHour), tzMin(tzMin)
            {
            }
            
            template <class Iter>
            void operator()(Iter, Iter) const
            {
                tzOffset = (tzHour * 60 + tzMin) * 60;
                if(tzIsNegative)
                    tzOffset = -tzOffset;
    
                //cout << "tzN " << tzIsNegative << endl
                //     << "tzH " << tzHour << endl
                //     << "tzM " << tzMin << endl
                //     << "tzO " << tzOffset << endl;
            }
        };
    
        struct AssignDate
        {
            int64_t & r;
            struct tm const & t;
            uint64_t const & usec;
            int const & parsedTimezone;
            int const & localTimezone;
            
            AssignDate(int64_t & r, struct tm const & t, uint64_t const & usec,
                       int const & parsedTimezone, int const & localTimezone) :
                r(r), t(t), usec(usec),
                parsedTimezone(parsedTimezone),
                localTimezone(localTimezone)
            {
            }
            
            template <class Iter>
            void operator()(Iter, Iter) const
            {
                //cout << "Y   " << t.tm_year << endl
                //     << "M   " << t.tm_mon << endl
                //     << "D   " << t.tm_mday << endl
                //     << "h   " << t.tm_hour << endl
                //     << "m   " << t.tm_min << endl
                //     << "s   " << t.tm_sec << endl
                //     << "us  " << usec << endl
                //     << "dst " << t.tm_isdst << endl
                //     << "PTZ " << parsedTimezone << endl
                //     << "LTZ " << localTimezone << endl;
    
                // Adjust tm struct
                struct tm t2 = t;
    
                // Year is offset from 1900
                t2.tm_year -= 1900;
                
                // Months is 0-11
                if(t2.tm_mon > 0)
                    t2.tm_mon -= 1;
    
                time_t utc = mktime(&t2) + (localTimezone - parsedTimezone);
                r = int64_t(utc) * 1000000 + usec;
            }
        };
    };
    
    boost::spirit::functor_parser<TimestampParser> const timestamp_p;

} // namespace parsing
} // namespace warp

#endif // WARP_PARSING_TIMESTAMP_H
