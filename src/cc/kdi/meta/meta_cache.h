//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-06-03
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
