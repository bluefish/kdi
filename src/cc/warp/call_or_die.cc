//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-12-02
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

#include <warp/call_or_die.h>
#include <warp/log.h>
#include <ex/exception.h>
#include <boost/bind.hpp>

using namespace warp;

namespace {

    void _callOrDie(boost::function<void ()> const & func,
                    std::string const & name, bool verbose)
    {
        if(verbose)
            log("%s starting", name);

        try {
            func();
        }
        catch(ex::Exception const & ex) {
            log("%s error: %s", name, ex);
            std::terminate();
        }
        catch(std::exception const & ex) {
            log("%s error: %s", name, ex.what());
            std::terminate();
        }
        catch(...) {
            log("%s error: unknown exception", name);
            std::terminate();
        }

        if(verbose)
            log("%s exiting", name);
    }

}

boost::function<void ()> warp::callOrDie(
    boost::function<void ()> const & func,
    std::string const & name, bool verbose)
{
    return boost::bind(&_callOrDie, func, name, verbose);
}
