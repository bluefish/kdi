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

#ifndef WARP_PARSING_TIMESTAMP_H
#define WARP_PARSING_TIMESTAMP_H

#include <warp/parsing/numeric.h>
#include <time.h>
#include <string.h>
#include <boost/spirit.hpp>

namespace warp {
namespace parsing {

    namespace details {

        using namespace boost::spirit;

        /// parse the second part of a timestamp as a float: SS[.s*]
        template <typename T>
        struct second_parser_policies
        {
            typedef uint_parser<T, 10, 2, 2>    n_parser_t;
            typedef uint_parser<T, 10, 1, -1>   frac_parser_t;
    
            static const bool allow_leading_dot  = false;
            static const bool allow_trailing_dot = true;
            static const bool expect_dot         = false;
    
            template <typename ScannerT>
            static typename match_result<ScannerT, nil_t>::type
            parse_sign(ScannerT& scan)
            { return scan.no_match(); }
    
            template <typename ScannerT>
            static typename parser_result<n_parser_t, ScannerT>::type
            parse_n(ScannerT& scan)
            { return n_parser_t().parse(scan); }
    
            template <typename ScannerT>
            static typename parser_result<chlit<>, ScannerT>::type
            parse_dot(ScannerT& scan)
            { return ch_p('.').parse(scan); }
    
            template <typename ScannerT>
            static typename parser_result<frac_parser_t, ScannerT>::type
            parse_frac_n(ScannerT& scan)
            { return frac_parser_t().parse(scan); }
    
            template <typename ScannerT>
            static typename match_result<ScannerT, nil_t>::type
            parse_exp(ScannerT& scan)
            { return scan.no_match(); }
    
            template <typename ScannerT>
            static typename match_result<ScannerT, nil_t>::type
            parse_exp_n(ScannerT& scan)
            { return scan.no_match(); }
        };
    
        boost::spirit::real_parser<double, second_parser_policies<double> > const second_p;
    }

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
            double sec = 0;
    
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
                              !( !ch_p(':') >> details::second_p[assign_a(sec)] )
                            )
                         ) >>
                        // Optional time zone
                        !( (ch_p('Z') | ch_p('z'))[assign_a(parsedTimezone, 0)][assign_a(t.tm_isdst, 0)] |
                           ( sign_p[assign_a(tzIsNegative)] >> 
                             uint2_p[assign_a(tzHour)] >>
                             !( !ch_p(':') >> uint2_p[assign_a(tzMin)] )
                           )[AssignTimezone(parsedTimezone, tzIsNegative, tzHour, tzMin)][assign_a(t.tm_isdst, 0)]
                         )
                    )[AssignDate(result, t, sec, parsedTimezone, localTimezone)]
                )
                .parse(scan)
                .length();
        }
    
    private:
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
            double const & sec;
            int const & parsedTimezone;
            int const & localTimezone;
            
            AssignDate(int64_t & r, struct tm const & t, double const & sec,
                       int const & parsedTimezone, int const & localTimezone) :
                r(r), t(t), sec(sec),
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
                //     << "s   " << sec << endl
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
                r = int64_t(utc) * 1000000 + int64_t(sec * 1e6);
            }
        };
    };
    
    boost::spirit::functor_parser<TimestampParser> const timestamp_p;

} // namespace parsing
} // namespace warp

#endif // WARP_PARSING_TIMESTAMP_H
