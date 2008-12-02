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
using namespace kdi;
using namespace warp;

namespace
{
    template <class T>
    class EmptyScan : public flux::Stream<T>
    {
    public:
        bool get(T & x) { return false; }
    };

    /// Placeholder fragment with empty data.  This is used in place
    /// of a DiskFragment if we're unable to load the disk fragment.
    class EmptyFragment : public Fragment
    {
        std::string uri;

    public:
        explicit EmptyFragment(std::string const & uri) : uri(uri) {}

        CellStreamPtr scan(ScanPredicate const & pred) const
        {
            CellStreamPtr p(new EmptyScan<Cell>);
            return p;
        }

        bool isImmutable() const
        {
            return true;
        }

        std::string getFragmentUri() const
        {
            return uri;
        }

        size_t getDiskSize(warp::Interval<std::string> const & rows) const
        {
            return 0;
        }
        
        flux::Stream< std::pair<std::string, size_t> >::handle_t
        scanIndex(warp::Interval<std::string> const & rows) const
        {
            flux::Stream< std::pair<std::string, size_t> >::handle_t p(
                new EmptyScan< std::pair<std::string, size_t> >
                );
            return p;
        }
    };
}


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
        try {
            ptr.reset(new DiskFragment(uri));
        }
        catch(std::exception const & ex) {
            log("ERROR: failed to load %s: %s", uri, ex.what());
            ptr.reset(new EmptyFragment(uri));
        }

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
