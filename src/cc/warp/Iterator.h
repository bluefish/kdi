//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-02
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

#ifndef WARP_ITERATOR_H
#define WARP_ITERATOR_H

namespace warp {

    template <class T>
    class Iterator
    {
    public:
        virtual ~Iterator() {}

        /// Move to the next element in the sequence.  Return true iff
        /// such an element exists, or false on end-of-sequence.
        virtual bool next() = 0;

        /// Get the current element in the sequence.  This method is only
        /// valid when the last call to next() has returned true.  If
        /// next() last returned false or hasn't yet been called, the
        /// current element is undefined.
        virtual T const & getCurrent() const = 0;
    };

} // namespace warp

#endif // WARP_ITERATOR_H
