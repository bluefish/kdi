//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-01
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

#ifndef WARP_SYSINFO_H
#define WARP_SYSINFO_H

#include <stddef.h>

namespace warp {

    struct PageCounts
    {
        size_t total;
        size_t resident;
        size_t shared;
        size_t text;
        size_t lib;
        size_t data;
        size_t dirty;
    };

    size_t getPageSize();
    bool readPageCounts(PageCounts * counts);

} // namespace warp

#endif // WARP_SYSINFO_H
