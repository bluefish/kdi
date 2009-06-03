//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-06-02
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

#ifndef WARP_WAITCALLBACK_H
#define WARP_WAITCALLBACK_H

#include <warp/Callback.h>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/shared_ptr.hpp>
#include <stdexcept>

namespace warp {

    class WaitCallback;

} // namespace warp

//----------------------------------------------------------------------------
// WaitCallback
//----------------------------------------------------------------------------
class warp::WaitCallback
    : public warp::Callback
{
public:
    WaitCallback() : triggered(false) {}

    virtual void done()
    {
        boost::mutex::scoped_lock lock(mutex);
        triggered = true;
        cond.notify_all();
    }

    virtual void error(std::exception const & err)
    {
        boost::mutex::scoped_lock lock(mutex);
        triggered = true;
        
        // It would be better to rethrow the error and try to capture
        // different error types, but that's tedious and better left
        // to library code.  It would be better to use said library
        // code (like Boost exception_ptr, or C++0x exception_ptr
        // support).
        this->err.reset(new std::runtime_error(err.what()));

        cond.notify_all();
    }

    void wait()
    {
        boost::mutex::scoped_lock lock(mutex);
        while(!triggered)
            cond.wait(lock);
        if(err)
            throw *err;
    }

private:
    boost::mutex mutex;
    boost::condition cond;
    boost::shared_ptr<std::exception> err;
    bool triggered;
};


#endif // WARP_WAITCALLBACK_H
