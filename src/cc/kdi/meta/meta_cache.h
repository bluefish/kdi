//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/meta/meta_cache.h $
//
// Created 2008/06/03
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_META_META_CACHE_H
#define KDI_META_META_CACHE_H

#include <kdi/table.h>
#include <warp/interval.h>
#include <warp/objectpool.h>
#include <boost/noncopyable.hpp>
#include <string>
#include <vector>

namespace kdi {
namespace meta {

    struct MetaEntry
    {
        // Rows: [firstKnownRow, lastRowFromMeta]
        warp::Interval<std::string> rows;

        // Row key from meta table
        std::string metaRow;

        // Location column from meta table
        std::string location;
    };

    class MetaCache;

} // namespace meta
} // namespace kdi

//----------------------------------------------------------------------------
// MetaCache
//----------------------------------------------------------------------------
class kdi::meta::MetaCache
    : private boost::noncopyable
{
    typedef std::vector<MetaEntry *> cache_t;

    TablePtr metaTable;
    warp::ObjectPool<MetaEntry> pool;
    cache_t cache;

    /// Update cache with a new authoritative location cell from the
    /// meta table.
    cache_t::iterator update(
        cache_t::iterator cursor,
        Cell const & locationCell,
        warp::IntervalPoint<std::string> const & firstRow);

public:
    explicit MetaCache(TablePtr const & metaTable);

    TablePtr const & getMetaTable() const { return metaTable; }

    MetaEntry const & lookup(strref_t table, strref_t row);
    void invalidate(MetaEntry const & entry); // unimplemented
};

#endif // KDI_META_META_CACHE_H
