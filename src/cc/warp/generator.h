//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-20
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

#ifndef WARP_GENERATOR_H
#define WARP_GENERATOR_H

#include <ex/exception.h>
#include <boost/shared_ptr.hpp>

namespace warp
{
    template <class T>
    class Generator
    {
    public:
        typedef T value_t;
        typedef Generator<T> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

    public:
        virtual ~Generator() {}

        /// Return the next item in the sequence.  May throw
        /// StopIteration if sequence is exhausted.
        virtual T next() = 0;

        /// Get the next item in sequence.  Returns true if item
        /// exists, or false on end of sequence (i.e. StopIteration
        /// was thrown).
        bool getNext(T & x)
        {
            try {
                x = next();
                return true;
            }
            catch(ex::StopIteration) {
                return false;
            }
        }
    };
}

#endif // WARP_GENERATOR_H
