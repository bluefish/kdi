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

#include <kdi/tablet/DiskFragmentCache.h>
#include <kdi/tablet/DiskFragment.h>
#include <warp/log.h>
#include <algorithm>

using namespace kdi::tablet;
using namespace warp;

//----------------------------------------------------------------------------
// DiskFragmentCache
//----------------------------------------------------------------------------
DiskFragmentCache::DiskFragmentCache() :
    nextPurgeSize(100)
{
}

FragmentPtr DiskFragmentCache::getDiskFragment(std::string const & uri)
{
    boost::mutex::scoped_lock lock(mutex);

    // Get the fragment out of the cache
    FragmentWeakPtr & wptr = fragmentMap[uri];

    // We hold a weak pointer, lock it and make sure it isn't null
    FragmentPtr ptr = wptr.lock();
    if(!ptr)
    {
        // Weak pointer is null -- need to load
        log("DiskFragmentCache: loading %s", uri);
        ptr.reset(new DiskFragment(uri));

        // Reset the weak pointer
        wptr = ptr;
    }

    // Purge stale cache entries if necessary
    if(fragmentMap.size() >= nextPurgeSize)
        purgeStaleEntries();

    return ptr;
}

void DiskFragmentCache::purgeStaleEntries()
{
    log("DiskFragmentCache: reached %d entries, purging",
        fragmentMap.size());

    // Walk the entire map and see if its pointer is still valid
    map_t::iterator i = fragmentMap.begin();
    while(i != fragmentMap.end())
    {
        if(i->second.expired())
            // Object expired, purge from cache
            fragmentMap.erase(i++);
        else
            // Keep and advance
            ++i;
    }
    
    // Set next purge to twice the current size
    nextPurgeSize = std::max(fragmentMap.size() * 2, size_t(100));

    log("DiskFragmentCache: purge complete, %d entries remain",
        fragmentMap.size());
}
