//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/meta/meta_cache.cc $
//
// Created 2008/06/03
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/meta/meta_cache.h>
#include <kdi/meta/meta_util.h>
#include <ex/exception.h>

using namespace kdi;
using namespace kdi::meta;
using namespace warp;
using namespace ex;

namespace
{
    struct CacheCmp
    {
        bool operator()(MetaEntry const * a, strref_t b) const {
            return a->metaRow < b;
        }
        bool operator()(strref_t a, MetaEntry const * b) const {
            return a < b->metaRow;
        }
    };
}

//----------------------------------------------------------------------------
// MetaCache
//----------------------------------------------------------------------------
MetaCache::cache_t::iterator MetaCache::update(
    cache_t::iterator cursor,
    Cell const & locationCell,
    warp::IntervalPoint<std::string> const & firstRow)
{
    // Invalidate everything between the cursor and the
    // lower-bound of the location cell.
    while(cursor != cache.end() &&
          (*cursor)->metaRow < locationCell.getRow())
    {
        pool.release(*cursor);
        cursor = cache.erase(cursor);
    }

    // If the cursor points to an entry for the location cell, we
    // only need to update the existing entry.  Otherwise, we must
    // make a new entry.
    if(cursor == cache.end() || locationCell.getRow() < (*cursor)->metaRow)
    {
        // Make a new cache entry
        cursor = cache.insert(cursor, pool.get());
        (*cursor)->metaRow = str(locationCell.getRow());
        (*cursor)->rows.setUpperBound(
            getTabletRowBound(locationCell.getRow()));
    }

    // Update existing cache entry
    (*cursor)->rows.setLowerBound(firstRow);
    (*cursor)->location = str(locationCell.getValue());

    // XXX Go back and invalidate entries that overlap this one (this
    // can happen when tablets merge).  Could this mess up the entry
    // we've just inserted?  Need to think through this.  However,
    // since the meta table is currently static, don't worry about it.

    return cursor;
}

MetaCache::MetaCache(TablePtr const & metaTable) :
    metaTable(metaTable)
{
    EX_CHECK_NULL(metaTable);
}

MetaEntry const & MetaCache::lookup(strref_t table, strref_t row)
{
    // Find the entry containing the row
    std::string metaRow = encodeMetaRow(table, row);
    cache_t::iterator i = std::lower_bound(cache.begin(), cache.end(), metaRow, CacheCmp());

    // Did we find it?
    if(i != cache.end() && (*i)->rows.contains(row, warp::less()))
        return **i;

    // Didn't find one -- update the cache

    // We'll need to re-scan the meta table to get the
    // authoritative answer.  We'll also scan ahead a bit while
    // we're at it.
    CellStreamPtr scan = metaLocationScan(metaTable, table, row);
        
    // The first item in the scan contains our row.
    Cell x;
    if(!scan->get(x))
        raise<RuntimeError>("no meta entry for table: %s", table);
    i = update(i, x, IntervalPoint<std::string>(str(row), PT_INCLUSIVE_LOWER_BOUND));

    // Remember the entry we need to return
    MetaEntry const * ret = *i;

    // Read-ahead in the scan a bit
    size_t const MAX_READAHEAD = 16;
    for(size_t readAhead = 0; readAhead < MAX_READAHEAD && scan->get(x); ++readAhead)
    {
        IntervalPoint<std::string> rowLowerBound =
            (*i)->rows.getUpperBound().getAdjacentComplement();
        i = update(++i, x, rowLowerBound);
    }

    // Return the appropriate entry
    return *ret;
}
