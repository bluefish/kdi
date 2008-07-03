//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-08-31
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

#ifndef WARP_MAPPED_REGION_H
#define WARP_MAPPED_REGION_H

#include <sys/mman.h>
#include <string>
#include <boost/noncopyable.hpp>

namespace warp
{
    class MappedRegion;
}

//----------------------------------------------------------------------------
// MappedRegion
//----------------------------------------------------------------------------
class warp::MappedRegion : private boost::noncopyable
{
    void * mem;
    size_t sz;
    int fd;

public:
    /// Memory map an entire file with the given protection and flags.
    /// The given file name is a URI, but it must map to a local path.
    MappedRegion(std::string const & uri, int prot, int flags);

    /// Memory map a segment of a file with the given protection and
    /// flags.  The given file name is a URI, but it must map to a
    /// local path.  The size of the region is given by \c sz, and the
    /// offset from the beginning of the file is given by \c offset.
    /// The offset must be a multiple of the system page size.
    MappedRegion(std::string const & uri, int prot, int flags,
                 size_t sz, off_t offset);

    /// Memory map an anonymous region with the given protection and
    /// flags.  The size of the region is given by \c sz.
    MappedRegion(int prot, int flags, size_t sz);

    /// Region is unmapped in destructor.
    ~MappedRegion();
    
    /// Scan through the entire mapped region and read every page.
    /// This gets page faults out of the way up front.
    void prefault() const;

    /// Scan through the sub-region and read every page.  This gets
    /// page faults out of the way up front.
    void prefault(size_t off, size_t len) const;

    /// Get the size of the mapped region.
    size_t size() const { return sz; }

    /// Get a pointer to the beginning of the mapped region.
    void * get() { return mem; }

    /// Get a pointer to the beginning of the mapped region.
    void const * get() const { return mem; }

    /// Get a pointer into the mapped region at the given position.
    void * get(off_t pos)
    {
        return static_cast<char *>(mem) + pos;
    }

    /// Get a pointer into the mapped region at the given position.
    void const * get(off_t pos) const
    {
        return static_cast<char const *>(mem) + pos;
    }

    /// Get a pointer to the beginning of the mapped region, cast to
    /// the given type.
    template <class T>
    T * getAs() { return static_cast<T *>(mem); }

    /// Get a pointer to the beginning of the mapped region, cast to
    /// the given type.
    template <class T>
    T const * getAs() const { return static_cast<T const *>(mem); }

    /// Get a pointer into the mapped region at the given position,
    /// cast to the given type.
    template <class T>
    T * getAs(off_t pos)
    {
        return static_cast<T *>(get(pos));
    }

    /// Get a pointer into the mapped region at the given position,
    /// cast to the given type.
    template <class T>
    T const * getAs(off_t pos) const
    {
        return static_cast<T const *>(get(pos));
    }
};


#endif // WARP_MAPPED_REGION_H
