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

#include <warp/timestamp.h>
#include <warp/parsing/numeric.h>
#include <string.h>
#include <boost/spirit.hpp>

//#define WARP_PARSING_TIMESTAMP_DEBUG
#ifdef WARP_PARSING_TIMESTAMP_DEBUG
#include <iostream>
#endif

namespace warp {
namespace parsing {

    /// Parse a timestamp and convert it into a warp::Timestamp.  The
    /// timestamp may either be an ISO8601 date time string or an
    /// integer literal prefixed by an '@'.
    class TimestampParser
    {
    public:
        typedef warp::Timestamp result_t;
    
        template <typename ScannerT>
        std::ptrdiff_t
        operator()(ScannerT const & scan, result_t & result) const
        {
            using namespace boost::spirit;

            int year = 1970;
            int month = 1;
            int day = 1;
            int hour = 0;
            int min = 0;
            int sec = 0;
            int32_t usec = 0;
            int32_t gmtoff = 0;
            bool hasTimezone = false;
    
            bool tzIsNegative = false;
            int tzHour = 0;
            int tzMin = 0;

            return
                (
                    ( // Integer literal
                        ch_p('@') >> int64_p[AssignLiteral(result)]
                    ) |
                    ( // ISO8601 date time
                        // Year
                        uint4_p[assign_a(year)] >> 
                        // Optional month
                        !( !ch_p('-') >> uint2_p[assign_a(month)] >>
                           // Optional day
                           !( !ch_p('-') >> uint2_p[assign_a(day)] )
                         ) >>
                        // Optional time value
                        !( (ch_p('T') | ch_p('t')) >>
                           // Hour
                           uint2_p[assign_a(hour)] >>
                           // Optional minute
                           !( !ch_p(':') >> uint2_p[assign_a(min)] >>
                              // Optional second
                              !( !ch_p(':') >> uint2_p[assign_a(sec)] >>
                                 // Optional microsecond
                                 !( ch_p('.') >> (+digit_p)[AssignMicroseconds(usec)] )
                               )
                            )
                         ) >>
                        // Optional time zone
                        !( (ch_p('Z') | ch_p('z'))[assign_a(gmtoff, 0)] |
                           ( sign_p[assign_a(tzIsNegative)] >> 
                             uint2_p[assign_a(tzHour)] >>
                             !( !ch_p(':') >> uint2_p[assign_a(tzMin)] )
                           )[AssignTimezone(gmtoff, tzIsNegative, tzHour, tzMin)]
                         )[assign_a(hasTimezone, true)]
                     )[AssignTimestamp(result,
                                       year, month, day,
                                       hour, min, sec,
                                       usec, gmtoff, hasTimezone)]
                )
                .parse(scan)
                .length();
        }
    
    private:
        struct AssignMicroseconds
        {
            int32_t & usec;

            AssignMicroseconds(int32_t & usec) : usec(usec) {}
            
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
            int32_t & tzOffset;
            bool const & tzIsNegative;
            int const & tzHour;
            int const & tzMin;
    
            AssignTimezone(int32_t & tzOffset,
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

#ifdef WARP_PARSING_TIMESTAMP_DEBUG    
                using namespace std;
                cout << "tzN " << tzIsNegative << endl
                     << "tzH " << tzHour << endl
                     << "tzM " << tzMin << endl
                     << "tzO " << tzOffset << endl;
#endif
            }
        };
    
        struct AssignTimestamp
        {
            result_t & r;
            int const & year;
            int const & month;
            int const & day;
            int const & hour;
            int const & min;
            int const & sec;
            int32_t const & usec;
            int32_t const & gmtoff;
            bool const & hasTimezone;

            AssignTimestamp(result_t & r,
                            int const & year,
                            int const & month,
                            int const & day,
                            int const & hour,
                            int const & min,
                            int const & sec,
                            int32_t const & usec,
                            int32_t const & gmtoff,
                            bool const & hasTimezone) :
                r(r),
                year(year),
                month(month),
                day(day),
                hour(hour),
                min(min),
                sec(sec),
                usec(usec),
                gmtoff(gmtoff),
                hasTimezone(hasTimezone)
            {
            }
            
            template <class Iter>
            void operator()(Iter, Iter) const
            {
#ifdef WARP_PARSING_TIMESTAMP_DEBUG
                using namespace std;
                cout << "Y   " << year << endl
                     << "M   " << month << endl
                     << "D   " << day << endl
                     << "h   " << hour << endl
                     << "m   " << min << endl
                     << "s   " << sec << endl
                     << "us  " << usec << endl
                     << "off " << gmtoff << endl
                     << "tz? " << (hasTimezone ? "true" : "false") << endl;
#endif
                
                if(hasTimezone)
                    r.set(year, month, day,
                          hour, min, sec,
                          usec, gmtoff);
                else
                    r.setLocal(year, month, day,
                               hour, min, sec,
                               usec);

#ifdef WARP_PARSING_TIMESTAMP_DEBUG
#endif
            }
        };

        class AssignLiteral
        {
            result_t & r;

        public:
            explicit AssignLiteral(result_t & r) : 
                r(r) {}

            void operator()(int64_t literal) const
            {
                r = warp::Timestamp::fromMicroseconds(literal);
            }
        };
    };
    
    boost::spirit::functor_parser<TimestampParser> const timestamp_p;

} // namespace parsing
} // namespace warp

#endif // WARP_PARSING_TIMESTAMP_H
