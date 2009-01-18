//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-15
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

#ifndef WARP_STATTRACKER_H
#define WARP_STATTRACKER_H

#include <warp/string_range.h>

namespace warp {

    class StatTracker
    {
    public:
        /// Set the named the named stat to the given value
        virtual void set(strref_t name, int64_t value) = 0;

        /// Add delta to the named stat
        virtual void add(strref_t name, int64_t delta) = 0;

    protected:
        ~StatTracker() {}
    };

    class NullStatTracker
        : public StatTracker
    {
    public:
        void set(strref_t name, int64_t value) {}
        void add(strref_t name, int64_t delta) {}
    };

} // namespace warp

#endif // WARP_STATTRACKER_H
