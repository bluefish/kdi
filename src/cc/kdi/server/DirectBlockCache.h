//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-07
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

#ifndef KDI_SERVER_DIRECTBLOCKCACHE_H
#define KDI_SERVER_DIRECTBLOCKCACHE_H

#include <kdi/server/BlockCache.h>

namespace kdi {
namespace server {

    class DirectBlockCache;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// DirectBlockCache
//----------------------------------------------------------------------------
class kdi::server::DirectBlockCache
    : public BlockCache
{
public:
    FragmentBlock const * getBlock(
        Fragment const * fragment, size_t blockAddr)
    {
        return fragment->loadBlock(blockAddr).release();
    }

    void releaseBlock(FragmentBlock const * block)
    {
        delete block;
    }
};

#endif // KDI_SERVER_DIRECTBLOCKCACHE_H
