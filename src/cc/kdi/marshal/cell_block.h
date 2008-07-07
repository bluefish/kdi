//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-11-07
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

#ifndef KDI_MARSHAL_CELL_BLOCK_H
#define KDI_MARSHAL_CELL_BLOCK_H

#include <warp/string_data.h>
#include <warp/offset.h>

namespace kdi {
namespace marshal {

    /// Row, column, timestamp key for a table Cell.
    struct CellKey
    {
        warp::StringOffset row;
        warp::StringOffset column;
        int64_t timestamp;
    };

    /// Contents of a table Cell.  A null value indicates an erasure
    /// for the given key.  This structure contains explicit alignment
    /// padding.
    struct CellData
    {
        CellKey key;
        warp::StringOffset value;     // null means erasure
        uint32_t __pad;
    };

    /// Ordered collection of Cells.
    struct CellBlock
    {
        enum {
            TYPECODE = WARP_PACK4('C','e','l','B'),
            VERSION = 0,
            FLAGS = 0,
            ALIGNMENT = 8,
        };

        warp::ArrayOffset<CellData> cells;
    };

} // namespace marshal
} // namespace kdi

#endif // KDI_MARSHAL_CELL_BLOCK_H
