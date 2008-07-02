//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/marshal/cell_block_builder.h#2 $
//
// Created 2007/11/09
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_MARSHAL_CELL_BLOCK_BUILDER_H
#define KDI_MARSHAL_CELL_BLOCK_BUILDER_H

#include <kdi/marshal/cell_block.h>
#include <kdi/strref.h>
#include <kdi/cell.h>
#include <warp/builder.h>
#include <warp/string_pool_builder.h>

namespace kdi {
namespace marshal {

    class CellBlockBuilder;

} // namespace marshal
} // namespace kdi

//----------------------------------------------------------------------------
// CellBlockBuilder
//----------------------------------------------------------------------------
class kdi::marshal::CellBlockBuilder
    : private boost::noncopyable
{
    warp::BuilderBlock * base;
    warp::BuilderBlock * arr;
    warp::StringPoolBuilder pool;
    size_t basePos;
    uint32_t nCells;

    warp::BuilderBlock * getString(strref_t s)
    {
        return pool.get(s);

        // warp::BuilderBlock * b = base->subblock(4);
        // *b << warp::StringData::wrap(s);
        // return b;
    }

    void append(warp::BuilderBlock * row,
                warp::BuilderBlock * column,
                int64_t timestamp,
                warp::BuilderBlock * value)
    {
        // Append CellData to array
        arr->appendOffset(row);         // key.row
        arr->appendOffset(column);      // key.column
        arr->append(timestamp);         // key.timestamp
        arr->appendOffset(value);       // value
        arr->append<uint32_t>(0);       // __pad

        // Update cells.length in main block
        ++nCells;
        base->write(basePos + 4, nCells);  // cells.length
    }

public:
    /// Create a CellBlockBuilder over the given BuilderBlock.
    explicit CellBlockBuilder(warp::BuilderBlock * builder) :
        pool(builder)
    {
        reset(builder);
    }

    /// Reset the builder with a new BuilderBlock.
    void reset(warp::BuilderBlock * builder)
    {
        BOOST_STATIC_ASSERT(CellBlock::VERSION == 0);

        EX_CHECK_NULL(builder);

        base = builder;
        basePos = base->size();
        arr = base->subblock(8);
        pool.reset(builder);
        nCells = 0;

        *base << *arr           // cells.offset
              << nCells;        // cells.length
    }

    /// Reset the builder and reuse the same BuilderBlock.
    void reset() { reset(getBuilder()); }

    /// Get the backing BuilderBlock.
    warp::BuilderBlock * getBuilder() const { return base; }

    /// Append a Cell to the current CellBlock.
    void appendCell(strref_t row, strref_t column, int64_t timestamp,
                    strref_t value)
    {
        append(getString(row),
               getString(column),
               timestamp,
               getString(value));
    }

    /// Append an erasure Cell to the current CellBlock.
    void appendErasure(strref_t row, strref_t column, int64_t timestamp)
    {
        append(getString(row),
               getString(column),
               timestamp,
               0);
    }

    /// Append a erasure Cell to the current CellBlock.
    void append(Cell const & x)
    {
        append(getString(x.getRow()),
               getString(x.getColumn()),
               x.getTimestamp(),
               x.isErasure() ? 0 : getString(x.getValue()));
    }

    /// Get approximate data size of current CellBlock.
    size_t getDataSize() const
    {
        return base->size() - basePos + arr->size() + pool.getDataSize();
    }

    /// Get number of Cells in current CellBlock.
    size_t getCellCount() const
    {
        return nCells;
    }
};

#endif // KDI_MARSHAL_CELL_BLOCK_BUILDER_H
