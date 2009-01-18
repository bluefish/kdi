//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-18
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

#include <kdi/local/index_cache.h>
#include <kdi/local/disk_table.h>
#include <boost/scoped_ptr.hpp>

using namespace kdi::local;

//----------------------------------------------------------------------------
// IndexCache::Load
//----------------------------------------------------------------------------
void IndexCache::Load::operator()(oort::Record & r, std::string const & fn) const
{
    oort::Record tmp;
    DiskTable::loadIndex(fn, tmp);
    r = tmp.clone();
}

//----------------------------------------------------------------------------
// IndexCache
//----------------------------------------------------------------------------
IndexCache::IndexCache(size_t maxSize) :
    cache(maxSize), tracker(0)
{
}

oort::Record * IndexCache::get(std::string const & fn)
{
    locked_t p(cache);
    oort::Record * r = p->get(fn);
    if(tracker)
    {
        tracker->set("IndexCache.size", p->size());
        tracker->set("IndexCache.count", p->count());
    }
    return r;
}

void IndexCache::release(oort::Record * r)
{
    locked_t p(cache);
    p->release(r);
    if(tracker)
    {
        tracker->set("IndexCache.size", p->size());
        tracker->set("IndexCache.count", p->count());
    }
}

void IndexCache::remove(std::string const & fn)
{
    locked_t p(cache);
    p->remove(fn);
    if(tracker)
    {
        tracker->set("IndexCache.size", p->size());
        tracker->set("IndexCache.count", p->count());
    }
}


namespace
{
    IndexCache * gptr()
    {
        size_t const CACHE_SIZE = size_t(2) << 30;
        static boost::scoped_ptr<IndexCache> p(
            new IndexCache(CACHE_SIZE));
        return p.get();
    }
}

void IndexCache::setTracker(warp::StatTracker * tracker)
{
    gptr()->tracker = tracker;
}

IndexCache * IndexCache::getGlobal()
{
    return gptr();
}
