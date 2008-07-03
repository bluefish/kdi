//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-09-20
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#include <kdi/table_factory.h>
#include <warp/uri.h>

using namespace kdi;
using namespace warp;
using namespace ex;
using namespace std;

TableFactory & TableFactory::get()
{
    static TableFactory f;
    return f;
}

void TableFactory::registerTable(string const & scheme,
                                 creator_t const & creator)
{
    if(reg.find(scheme) != reg.end())
        raise<RuntimeError>(
            "table scheme has already been registered: %s", scheme);

    reg[scheme] = creator;
}

TablePtr TableFactory::create(string const & uri) const
{
    Uri u(wrap(uri));

    // Find the table creator corresponding to the URI scheme
    map_t::const_iterator it = reg.find(str(u.scheme));
    if(it == reg.end())
    {
        // Didn't find it.  Maybe it is a compound scheme.  Try the
        // first component (up to the first '+').
        char const * sep = std::find(u.scheme.begin(), u.scheme.end(), '+');
        it = reg.find(string(u.scheme.begin(), sep));

        if(it == reg.end())
        {
            // Still didn't find it.  No go.
            raise<RuntimeError>("unknown table scheme '%s': %s",
                                u.scheme, uri);
        }
    }

    // Invoke the creator and return the result
    return it->second(uri);
}
