//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/parsing/quoted_str.h $
//
// Created 2008/03/21
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
