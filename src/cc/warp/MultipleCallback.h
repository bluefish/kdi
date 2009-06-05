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

#ifndef WARP_MULTIPLECALLBACK_H
#define WARP_MULTIPLECALLBACK_H

#include <warp/Callback.h>
#include <boost/thread/mutex.hpp>
#include <vector>
#include <string>

namespace warp {

    class MultipleCallback;

} // namespace warp

//----------------------------------------------------------------------------
// MultipleCallback
//----------------------------------------------------------------------------
class warp::MultipleCallback
    : public warp::Callback
{
public:
    explicit MultipleCallback(warp::Callback * cb) :
        cb(cb), count(1) {}

    MultipleCallback(warp::Callback * cb, size_t count) :
        cb(cb), count(count) {}

    void incrementCount()
    {
        boost::mutex::scoped_lock lock(mutex);
        ++count;
    }

    virtual void done() throw();
    virtual void error(std::exception const & err) throw();

private:
    void maybeFinish();

private:
    warp::Callback * cb;
    size_t count;
    std::vector<std::string> errors;
    boost::mutex mutex;
};

#endif // WARP_MULTIPLECALLBACK_H
