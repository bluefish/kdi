//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-08
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

#ifndef KDI_LOCAL_TABLE_TYPES_H
#define KDI_LOCAL_TABLE_TYPES_H

#include <warp/string_data.h>
#include <warp/offset.h>
#include <warp/bloom_filter.h>
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
    struct IndexEntryV0 {
        CellKey startKey;
        uint64_t blockOffset;  // from beginning of file
    };

    uint32_t const BLOOM_N_BITS = 1024;
    size_t const BLOOM_DISK_SIZE = BLOOM_N_BITS/8;
    uint32_t const BLOOM_SEEDS[] = { 
        1016839685u, 2408313752u, 
        1822408320u, 1850600840u 
    };
    size_t const BLOOM_N_SEEDS = sizeof(BLOOM_SEEDS) / sizeof(*BLOOM_SEEDS);

    // Richer index format
    struct IndexEntryV1
    {
        CellKey startKey;
        uint64_t blockOffset;
        uint8_t colPrefixFilter[BLOOM_DISK_SIZE];
        int64_t lowestTime;
        int64_t highestTime;
        uint32_t numCells;
        uint32_t numErasures;
        uint32_t blockChecksum; // Adler-32

        bool hasColPrefix(strref_t x) const {
            return warp::BloomFilter::contains(BLOOM_N_SEEDS, BLOOM_SEEDS, 
                BLOOM_N_BITS, colPrefixFilter, x);
        }
    };

    /// Index of CellBlock records in the file.
    struct BlockIndexV0
    {
        enum {
            TYPECODE = WARP_PACK4('C','B','I','x'),
            VERSION = 0,
            FLAGS = 0,
            ALIGNMENT = 8,
        };

        warp::ArrayOffset<IndexEntryV0> blocks;
    };

    // Index of CellBlock records using the new format
    struct BlockIndexV1
    {
        enum {
            TYPECODE = WARP_PACK4('C','B','I','x'),
            VERSION = 1,
            FLAGS = 0,
            ALIGNMENT = 8
        };

        warp::ArrayOffset<IndexEntryV1> blocks;
    };

    // Trailer for a disk table file.
    struct TableInfoV0
    {
        enum {
            TYPECODE = WARP_PACK4('T','N','f','o'),
            VERSION = 0,
            FLAGS = 0,
            ALIGNMENT = 8,
        };

        uint64_t indexOffset;   // from beginning of file
    
        TableInfoV0() {}
        explicit TableInfoV0(uint64_t off) : indexOffset(off) {}
    };

    // Trailer for a disk table file.
    struct TableInfo
    {
        enum {
            TYPECODE = WARP_PACK4('T','N','f','o'),
            VERSION = 1,
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
