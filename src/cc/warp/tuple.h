//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/tuple.h $
//
// Created 2008/03/17
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_TUPLE_H
#define WARP_TUPLE_H

#include <boost/tuple/tuple.hpp>

namespace warp {

    using boost::tuple;
    using boost::make_tuple;
    using boost::tie;

    typedef boost::tuples::null_type null_type;

} // namespace warp

#endif // WARP_TUPLE_H
