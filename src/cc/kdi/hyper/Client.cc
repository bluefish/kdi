//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-08-21
// 
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the Free Software
// Foundation; either version 2 of the License, or any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
// details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

// Silly Hypertable
#include <Common/Compat.h>

#include <kdi/hyper/Client.h>
#include <boost/thread/mutex.hpp>
#include <stdlib.h>
#include <ex/exception.h>

using namespace kdi;
using namespace kdi::hyper;

::Hypertable::ClientPtr const & kdi::hyper::getClient()
{
    static boost::mutex mutex;
    boost::mutex::scoped_lock lock(mutex);

    static ::Hypertable::ClientPtr client;
    if(!client)
    {
        char const * installDir = getenv("HYPERTABLE_HOME");
        if(!installDir)
        {
            using namespace ex;
            raise<RuntimeError>("Environment variable HYPERTABLE_HOME not set");
        }
        
        client = new ::Hypertable::Client(installDir);
    }

    return client;
}
