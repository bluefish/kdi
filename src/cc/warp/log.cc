//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/log.cc $
//
// Created 2008/03/11
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <warp/log.h>
#include <warp/timestamp.h>
#include <boost/thread/mutex.hpp>
#include <iostream>

using namespace warp;

void warp::log(strref_t msg)
{
    static boost::mutex mutex;
    boost::mutex::scoped_lock l(mutex);

    std::cerr << Timestamp::now() << ": " << msg << std::endl;
}
