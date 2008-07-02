//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/log.h $
//
// Created 2008/03/11
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
/// @file Primitive logging facilities.  At the moment, this is just a
/// convenient way to do synchronized logging to cerr.  At some point,
/// it might become an interface layer for a logging package like
/// log4cpp.
//----------------------------------------------------------------------------

#ifndef WARP_LOG_H
#define WARP_LOG_H

#include <warp/strref.h>
#include <boost/format.hpp>

namespace warp {
    
    /// Log a line to stderr.  This function is thread-safe and will
    /// prevent threads from interleaving log messages.  The log
    /// message should not contain newlines.
    void log(strref_t msg);

    /// Log a line with argument substitution.
    template <class A1>
    void log(char const * fmt, A1 const & a1)
    {
        log((boost::format(fmt) % a1).str());
    }

    /// Log a line with argument substitution.
    template <class A1, class A2>
    void log(char const * fmt, A1 const & a1, A2 const & a2)
    {
        log((boost::format(fmt) % a1 % a2).str());
    }

    /// Log a line with argument substitution.
    template <class A1, class A2, class A3>
    void log(char const * fmt, A1 const & a1, A2 const & a2, A3 const & a3)
    {
        log((boost::format(fmt) % a1 % a2 % a3).str());
    }

    /// Log a line with argument substitution.
    template <class A1, class A2, class A3, class A4>
    void log(char const * fmt, A1 const & a1, A2 const & a2, A3 const & a3,
             A4 const & a4)
    {
        log((boost::format(fmt) % a1 % a2 % a3 % a4).str());
    }

    /// Log a line with argument substitution.
    template <class A1, class A2, class A3, class A4, class A5>
    void log(char const * fmt, A1 const & a1, A2 const & a2, A3 const & a3,
             A4 const & a4, A5 const & a5)
    {
        log((boost::format(fmt) % a1 % a2 % a3 % a4 % a5).str());
    }

} // namespace warp

#endif // WARP_LOG_H
