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

#include <warp/MultipleCallback.h>
#include <sstream>

using namespace warp;

//----------------------------------------------------------------------------
// MultipleCallback
//----------------------------------------------------------------------------
void MultipleCallback::done()
{
    boost::mutex::scoped_lock lock(mutex);
    maybeFinish();
}

void MultipleCallback::error(std::exception const & err)
{
    boost::mutex::scoped_lock lock(mutex);
    errors.push_back(std::string(err.what()));
    maybeFinish();
}

void MultipleCallback::maybeFinish()
{
    --count;
    if(count)
        return;

    if(errors.empty())
        cb->done();
    else
    {
        std::ostringstream oss;
        oss << errors.size() << " error(s):";
        for(std::vector<std::string>::const_iterator i = errors.begin();
            i != errors.end(); ++i)
        {
            oss << std::endl << *i;
        }
        cb->error(std::runtime_error(oss.str()));
    }
    delete this;
}
