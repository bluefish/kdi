//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-17
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

#ifndef KDI_LOCAL_INDEX_CACHE_H
#define KDI_LOCAL_INDEX_CACHE_H

#include <warp/lru_cache.h>
#include <warp/synchronized.h>
#include <oort/record.h>
#include <string>
#include <boost/noncopyable.hpp>

namespace kdi {
namespace local {

    class IndexCache;
    class CacheRecord;

} // namespace local
} // namespace kdi

//----------------------------------------------------------------------------
// IndexCache
//----------------------------------------------------------------------------
class kdi::local::IndexCache
    : private boost::noncopyable
{
    struct Load {
        void operator()(oort::Record & r, std::string const & fn) const;
    };

    typedef warp::LruCache<std::string, oort::Record, Load,
                           oort::record_size> cache_t;
    typedef warp::Synchronized<cache_t> scache_t;
    typedef warp::LockedPtr<cache_t> locked_t;
        
    scache_t cache;

public:
    explicit IndexCache(size_t maxSize) :
        cache(maxSize) {}

    oort::Record * get(std::string const & fn)
    {
        return locked_t(cache)->get(fn);
    }

    void release(oort::Record * r)
    {
        locked_t(cache)->release(r);
    }

    void remove(std::string const & fn)
    {
        locked_t(cache)->remove(fn);
    }

    static IndexCache * get();
};

//----------------------------------------------------------------------------
// CacheRecord
//----------------------------------------------------------------------------
class kdi::local::CacheRecord
    : private boost::noncopyable
{
    IndexCache * cache;
    oort::Record * record;
        
public:
    CacheRecord() : cache(0), record(0) {}

    CacheRecord(IndexCache * cache, std::string const & fn) :
        cache(cache), record(cache->get(fn)) {}

    ~CacheRecord()
    {
        if(cache)
            cache->release(record);
    }

    template <class T>
    T const * cast() const { return record->cast<T>(); }

    template <class T>
    T const * as() const { return record->as<T>(); }

    size_t getLength() const { return record->getLength(); }
};

#endif // KDI_LOCAL_INDEX_CACHE_H
