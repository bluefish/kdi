//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-05-04
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef WARP_BUFFER_H
#define WARP_BUFFER_H

#include "file.h"
#include <boost/utility.hpp>
#include <algorithm>
#include <string.h>
#include <assert.h>

namespace warp
{
    class Buffer;
}


//----------------------------------------------------------------------------
// Buffer
//----------------------------------------------------------------------------
class warp::Buffer : public boost::noncopyable
{
public:
    typedef char value_type;
    typedef char * iterator;
    typedef char const * const_iterator;
    typedef ptrdiff_t difference_type;
    typedef size_t size_type;

private:
    char * buf;                 // Beginning of buffer
    char * cap;                 // End of buffer
    char * pos;                 // Position in buffered data
    char * lim;                 // End of buffered data
    
public:
    /// Construct a buffer with zero capacity.
    Buffer() :
        buf(0),
        cap(0),
        pos(0),
        lim(0)
    {
    }

    /// Construct a buffer with \c sz byte capacity.
    Buffer(size_type sz) :
        buf(sz > 0 ? new char[sz] : 0),
        cap(buf + sz),
        pos(buf),
        lim(cap)
    {
    }

    /// Free memory associated with buffer.
    ~Buffer()
    {
        delete[] buf;
    }

    /// Get the total capacity of the buffer.
    size_type capacity() const { return cap - buf; }

    /// Check if buffer is empty (current position is equal to limit).
    bool empty() const { return pos == lim; }

    /// Get the number of bytes between the current position and the
    /// beginning of the buffer.
    size_type consumed() const { return pos - buf; }

    /// Get the number of bytes between the buffer limit and the
    /// current position.
    size_type remaining() const { return lim - pos; }

    /// Get beginning of buffer.
    iterator begin()    { return buf; }

    /// Get current position in buffer.
    iterator position() { return pos; }

    /// Get limit in buffer.
    iterator limit()    { return lim; }

    /// Get end of buffer.
    iterator end()      { return cap; }

    /// Get beginning of buffer.
    const_iterator begin()    const { return buf; }

    /// Get current position in buffer.
    const_iterator position() const { return pos; }

    /// Get limit in buffer.
    const_iterator limit()    const { return lim; }

    /// Get end of buffer.
    const_iterator end()      const { return cap; }

    /// Swap contents with another Buffer object.
    void swap(Buffer & o)
    {
        std::swap(buf, o.buf);
        std::swap(cap, o.cap);
        std::swap(pos, o.pos);
        std::swap(lim, o.lim);
    }

    /// Reserve enough space to put at least \c sz more bytes.  If
    /// remaining buffer space is already sufficient, do nothing.
    /// Otherwise, grow the buffer by an amount that at least doubles
    /// the total capacity.  Any data already in the buffer between
    /// begin() and position() will be preserved.
    void reserve(size_t sz)
    {
        if(remaining() < sz)
        {
            Buffer tmp(std::max(capacity() << 1, consumed() + sz));
            flip();
            tmp.put(*this);
            swap(tmp);
        }
    }

    /// Set position of buffer.
    void position(iterator p)
    {
        assert(p >= buf && p <= lim);
        pos = p;
    }

    /// Set limit of buffer.
    void limit(iterator l)
    {
        assert(l >= pos && l < cap);
        lim = l;
    }

    /// Set position to beginning of buffer and limit to end of
    /// buffer.
    void clear()
    {
        pos = buf;
        lim = cap;
    }

    /// Set limit to position, and position to beginning of buffer.
    void flip()
    {
        lim = pos;
        pos = buf;
    }

    /// Move any data between position and limit to beginning of
    /// buffer, set position to end of data, and limit to end of
    /// buffer.
    void compact()
    {
        if(size_t rem = lim-pos)
        {
            memmove(buf, pos, rem);
            pos = buf + rem;
        }
        else
        {
            pos = buf;
        }
        lim = cap;
    }

    /// Get the next byte in buffer, updating position.
    char get()
    {
        assert(remaining() > 0);
        return *pos++;
    }

    /// Get the next \c sz bytes in buffer, updating position.
    void get(void * dst, size_t sz)
    {
        assert(sz <= remaining());
        memcpy(dst, pos, sz);
        pos += sz;
    }

    /// Put a byte in the buffer, updating position.
    void put(char x)
    {
        assert(remaining() > 0);
        *pos++ = x;
    }

    /// Put \c sz bytes in the buffer, updating position.
    void put(void const * src, size_t sz)
    {
        assert(sz <= remaining());
        memcpy(pos, src, sz);
        pos += sz;
    }

    /// Put the contents of \c buf in the buffer, updating position.
    void put(Buffer & buf)
    {
        assert(buf.remaining() <= remaining());
        size_t sz = buf.lim - buf.pos;
        memcpy(pos, buf.pos, sz);
        pos += sz;
        buf.pos += sz;
    }

    /// Read bytes from \c fp into the buffer, updating position.
    size_t read(FilePtr const & fp)
    {
        size_t sz = fp->read(pos, lim - pos);
        pos += sz;
        return sz;
    }

    /// Write bytes from buffer to \c fp, updating position.
    size_t write(FilePtr const & fp)
    {
        size_t sz = fp->write(pos, lim - pos);
        pos += sz;
        return sz;
    }
};


#endif // WARP_BUFFER_H
