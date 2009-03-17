//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-02-27
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

#ifndef KDI_SERVER_BLOCKCACHE_H
#define KDI_SERVER_BLOCKCACHE_H

#include <stddef.h>

namespace kdi {
namespace server {

    class BlockCache;

    // Forward declaration
    class Fragment;
    class FragmentBlock;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// BlockCache
//----------------------------------------------------------------------------
class kdi::server::BlockCache
{
public:
    /// Get the FragmentBlock at blockAddr for the given Fragment.
    /// The returned block must eventually be released with a balanced
    /// call to releaseBlock().
    virtual FragmentBlock const * getBlock(
        Fragment const * fragment, size_t blockAddr) = 0;

    /// Release a FragmentBlock previously acquired by getBlock().
    virtual void releaseBlock(FragmentBlock const * block) = 0;

protected:
    ~BlockCache() {}
};


#endif // KDI_SERVER_BLOCKCACHE_H
