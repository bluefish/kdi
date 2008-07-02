//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/table_factory.cc#1 $
//
// Created 2007/09/20
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
