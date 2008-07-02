//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/parsing/numeric.h $
//
// Created 2008/03/21
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_PARSING_NUMERIC_H
#define WARP_PARSING_NUMERIC_H

#include <boost/spirit.hpp>
#include <stdint.h>

namespace warp {
namespace parsing {

    using boost::spirit::int_parser;
    using boost::spirit::uint_parser;

    int_parser<int64_t, 10, 1, -1> const int64_p = int_parser<int64_t, 10, 1, -1>();
    uint_parser<int, 10, 2, 2> const uint2_p = uint_parser<int, 10, 2, 2>();
    uint_parser<int, 10, 4, 4> const uint4_p = uint_parser<int, 10, 4, 4>();

} // namespace parsing
} // namespace warp

#endif // WARP_PARSING_NUMERIC_H
