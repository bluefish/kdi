//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-11-20
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

#include <kdi/tablet/CachedFragmentLoader.h>
#include <warp/log.h>
#include <ex/exception.h>
#include <algorithm>

using namespace kdi::tablet;
using namespace kdi;
using namespace warp;


//----------------------------------------------------------------------------
// CachedFragmentLoader
//----------------------------------------------------------------------------
CachedFragmentLoader::CachedFragmentLoader(FragmentLoader * loader) :
    loader(loader),
    nextPurgeSize(100)
{
    EX_CHECK_NULL(loader);
}

FragmentPtr CachedFragmentLoader::load(std::string const & uri) const
{
    boost::mutex::scoped_lock lock(mutex);

    // Get the fragment out of the cache
    FragmentWeakPtr & wptr = cache[uri];

    // We hold a weak pointer, lock it and make sure it isn't null
    FragmentPtr ptr = wptr.lock();
    if(!ptr)
    {
        // Weak pointer is null -- need to load
        ptr = loader->load(uri);

        // Reset the weak pointer
        wptr = ptr;
    }

    // Purge stale cache entries if necessary
    if(cache.size() >= nextPurgeSize)
        purgeStaleEntries();

    return ptr;
}

void CachedFragmentLoader::purgeStaleEntries() const
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
