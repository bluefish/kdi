//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/marshal/cell_block.h#1 $
//
// Created 2007/11/07
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
