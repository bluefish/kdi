//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/local/table_types.h#1 $
//
// Created 2007/10/08
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_LOCAL_TABLE_TYPES_H
#define KDI_LOCAL_TABLE_TYPES_H

#include <warp/string_data.h>
#include <warp/offset.h>
#include <kdi/marshal/cell_block.h>

namespace kdi {
namespace local {
namespace disk {

    // Simple structured format.  This can use string pooling like
    // SDStore's StaticTable.  This format is CPU-efficient, but not
    // space-efficient.  It is reasonable for tables that need to be
    // pinned into memory.
    //
    // Table file format:
    //   1+  Record of CellBlock
    //   1   Record of BlockIndex
    //   1   Record of TableInfo
    //   <EOF>
    //
    // The way to read the file is to seek to the TableInfo record at
    // 10+sizeof(TableInfo) bytes before EOF, read that, then find and
    // read the BlockIndex.

    using kdi::marshal::CellKey;
    using kdi::marshal::CellData;
    using kdi::marshal::CellBlock;

    /// Mapping from a beginning CellKey to a CellBlock offset.
    struct IndexEntry
    {
        CellKey startKey;
        uint64_t blockOffset;  // from beginning of file
    };

    /// Index of CellBlock records in the file.
    struct BlockIndex
    {
        enum {
            TYPECODE = WARP_PACK4('C','B','I','x'),
            VERSION = 0,
            FLAGS = 0,
            ALIGNMENT = 8,
        };

        warp::ArrayOffset<IndexEntry> blocks;
    };

    /// Trailer for a disk table file.
    struct TableInfo
    {
        enum {
            TYPECODE = WARP_PACK4('T','N','f','o'),
            VERSION = 0,
            FLAGS = 0,
            ALIGNMENT = 8,
        };

        uint64_t indexOffset;   // from beginning of file

        TableInfo() {}
        explicit TableInfo(uint64_t off) : indexOffset(off) {}
    };

} // namespace disk
} // namespace local
} // namespace kdi

#endif // KDI_LOCAL_TABLE_TYPES_H
