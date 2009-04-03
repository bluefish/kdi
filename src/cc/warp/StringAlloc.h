//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-31
//
// This file is part of the warp library.
//
// The warp library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
//
// The warp library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef WARP_STRINGALLOC_H
#define WARP_STRINGALLOC_H

#include <boost/noncopyable.hpp>
#include <stddef.h>

namespace warp {

    class StringAlloc;

} // namespace warp

//----------------------------------------------------------------------------
// StringAlloc
//----------------------------------------------------------------------------
class warp::StringAlloc
    : private boost::noncopyable
{
public:
    enum { DEFAULT_BLOCK_SIZE = 128<<10 };

public:
    /// Construct a new string allocator.  String memory will be
    /// allocated out of large-ish blocks.  The given minSz parameter
    /// is used to decide how large to make each block.
    explicit StringAlloc(size_t minSz=DEFAULT_BLOCK_SIZE)
        : head(0), minSz(minSz) {}

    ~StringAlloc() { freeAll(); }

    /// Get (sz) bytes of memory, allocating a new block if necessary.
    char * alloc(size_t sz)
    {
        if(!head || head->remaining < sz)
            expand(sz);
        head->remaining -= sz;
        return reinterpret_cast<char *>(head + 1) + head->remaining;
    }

    /// Release all memory previously returned by alloc.
    void freeAll()
    {
        while(head)
        {
            BlockHeader * next = head->next;
            delete[] reinterpret_cast<char *>(head);
            head = next;
        }
    }

private:
    struct BlockHeader
    {
        size_t remaining;
        BlockHeader * next;
    };

private:
    void expand(size_t sz)
    {
        if(sz < minSz)
            sz = minSz;
        char * buf = new char[sizeof(BlockHeader) + sz];
        BlockHeader * block = reinterpret_cast<BlockHeader *>(buf);
        block->remaining = sz;
        block->next = head;
        head = block;
    }

private:
    BlockHeader * head;
    size_t minSz;
};

#endif // WARP_STRINGALLOC_H
