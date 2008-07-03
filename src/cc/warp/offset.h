//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2005 Josh Taylor (Kosmix Corporation)
// Created 2005-12-05
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

#ifndef WARP_OFFSET_H
#define WARP_OFFSET_H

#include <stdint.h>
#include <assert.h>
#include <boost/static_assert.hpp>
#include <boost/utility.hpp>

namespace warp
{
    //------------------------------------------------------------------------
    // OffsetBase
    //------------------------------------------------------------------------
    template <class OffsetType=int32_t>
    class OffsetBase : public boost::noncopyable
    {
    public:
        enum { NULL_OFFSET = 1 };

    private:
        BOOST_STATIC_ASSERT(sizeof(OffsetType) >= 2);
        OffsetType offset;

    public:
        OffsetBase() {}
        
        /// Get offset value
        OffsetType const & getOffset() const { return offset; }

        /// Check if offset is null
        bool isNull() const { return offset == NULL_OFFSET; }
        operator bool() const { return !isNull(); }

        /// Get raw pointer address for non-null offsets
        char const * getrawnz() const {
            return reinterpret_cast<char const *>(&offset) + offset;
        }

        /// Get raw pointer address, handling null correctly
        char const * getraw() const { return isNull() ? 0 : getrawnz(); }

        // Pointer-like comparison operators
        template <class OT> bool operator==(OffsetBase<OT> const & o) const {
            return getraw() == o.getraw();
        }
        template <class OT> bool operator!=(OffsetBase<OT> const & o) const {
            return getraw() != o.getraw();
        }
        template <class OT> bool operator<(OffsetBase<OT> const & o) const {
            return getraw() < o.getraw();
        }
        template <class OT> bool operator<=(OffsetBase<OT> const & o) const {
            return getraw() <= o.getraw();
        }
        template <class OT> bool operator>(OffsetBase<OT> const & o) const {
            return getraw() > o.getraw();
        }
        template <class OT> bool operator>=(OffsetBase<OT> const & o) const {
            return getraw() >= o.getraw();
        }
    };


    //------------------------------------------------------------------------
    // Offset
    //------------------------------------------------------------------------
    template <class TargetType, class OffsetType=int32_t>
    class Offset : public OffsetBase<OffsetType>
    {
    public:
        TargetType const * getnz() const {
            return reinterpret_cast<TargetType const *>(this->getrawnz());
        }
        TargetType const * get() const { return this->isNull() ? 0 : getnz(); }
        TargetType const * operator->() const { return getnz(); }
        TargetType const & operator*() const { return *getnz(); }
    };


    //------------------------------------------------------------------------
    // ArrayOffset
    //------------------------------------------------------------------------
    template <class TargetType,
              class OffsetType = int32_t,
              class SizeType = uint32_t>
    class ArrayOffset : private Offset<TargetType, OffsetType>
    {
        SizeType length;

    public:
        TargetType const * begin() const { return this->getnz(); }
        TargetType const * end() const { return begin() + length; }

        TargetType const & operator[](int idx) const {
            assert(idx >= 0 && (SizeType)idx < length);
            return this->get()[idx];
        }

        size_t size() const { return length; }
        bool empty() const { return length == 0; }
    };

}

#endif // WARP_OFFSET_H
