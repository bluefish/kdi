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

#ifndef WARP_PARSING_QUOTED_STR_H
#define WARP_PARSING_QUOTED_STR_H

#include <warp/strutil.h>
#include <string>
#include <boost/spirit.hpp>

namespace warp {
namespace parsing {

    class QuotedStringParser
    {
    public:
        typedef std::string result_t;
        
        template <typename ScannerT>
        std::ptrdiff_t
        operator()(ScannerT const & scan, result_t & result) const
        {
            using namespace boost::spirit;
            return
                (
                    confix_p( '"', *(!ch_p('\\') >> anychar_p),  '"') |
                    confix_p('\'', *(!ch_p('\\') >> anychar_p), '\'')
                )
                [
                    Assign(result)
                ]
                .parse(scan)
                .length();
        }
    
    private:
        struct Assign
        {
            result_t & result;
            explicit Assign(result_t & result) : result(result) {}

            template <class Iter>
            void operator()(Iter begin, Iter end) const
            {
                result.clear();
                ++begin;
                --end;
                while(begin != end)
                {
                    if(*begin == '\\')
                        begin = decodeEscapeSequence(++begin, end, result);
                    else
                        result.push_back(*begin++);
                }
            }
        };
    };
    
    boost::spirit::functor_parser<QuotedStringParser> const quoted_str_p;

} // namespace parsing
} // namespace warp

#endif // WARP_PARSING_QUOTED_STR_H
