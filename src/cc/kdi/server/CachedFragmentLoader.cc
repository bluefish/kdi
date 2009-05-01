//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-01
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

#include <kdi/server/CachedFragmentLoader.h>
#include <warp/log.h>
#include <algorithm>
#include <cassert>

using namespace kdi::server;
using namespace warp;

//----------------------------------------------------------------------------
// CachedFragmentLoader
//----------------------------------------------------------------------------
CachedFragmentLoader::CachedFragmentLoader(FragmentLoader * baseLoader) :
    baseLoader(baseLoader),
    nextPurgeSize(100)
{
    assert(baseLoader);
}

FragmentCPtr CachedFragmentLoader::load(std::string const & filename)
{
    boost::mutex::scoped_lock lock(mutex);

    // Get the fragment out of the cache
    FragmentWeakCPtr & wptr = cache[filename];

    // We hold a weak pointer, lock it and make sure it isn't null
    FragmentCPtr ptr = wptr.lock();
    if(!ptr)
    {
        // Weak pointer is null -- need to load
        ptr = baseLoader->load(filename);

        // Reset the weak pointer
        wptr = ptr;
    }

    // Purge stale cache entries if necessary
    if(cache.size() >= nextPurgeSize)
        purgeStaleEntries();

    return ptr;
}

void CachedFragmentLoader::purgeStaleEntries()
{
    log("CachedFragmentLoader: reached %d entries, purging",
        cache.size());

    // Walk the entire map and see if its pointer is still valid
    map_t::iterator i = cache.begin();
    while(i != cache.end())
    {
        if(i->second.expired())
            // Object expired, purge from cache
            i = cache.erase(i);
        else
            // Keep and advance
            ++i;
    }
    
    // Set next purge to twice the current size
    nextPurgeSize = std::max(cache.size() * 2, size_t(100));

    log("CachedFragmentLoader: purge complete, %d entries remain",
        cache.size());
}
