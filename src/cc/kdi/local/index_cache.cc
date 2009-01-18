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
IndexCache * IndexCache::get()
{
    size_t const CACHE_SIZE = size_t(2) << 30;

    static boost::scoped_ptr<IndexCache> x(
        new IndexCache(CACHE_SIZE));
    
    return x.get();
}
