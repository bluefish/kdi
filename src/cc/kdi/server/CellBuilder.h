//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/server/CellBuilder.h $
//
// Created 2009/02/27
//
// Copyright 2009 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_SERVER_CELLBUILDER_H
#define KDI_SERVER_CELLBUILDER_H

#include <kdi/CellKey.h>
#include <kdi/marshal/cell_block_builder.h>
#include <warp/builder.h>
#include <vector>

namespace kdi {
namespace server {

    class CellBuilder;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// CellBuilder
//----------------------------------------------------------------------------
class kdi::server::CellBuilder
{
    warp::Builder builder;
    kdi::marshal::CellBlockBuilder cells;

public:
    CellBuilder() :
        builder(),
        cells(&builder)
    {
    }

    /// Clear the builder and prepare for a new batch.
    void reset()
    {
        builder.reset();
        cells.reset();
    }

    /// Write the encoded results into a buffer.  Returns the number
    /// of cells in output.  If there is at least one cell in the
    /// output, the last key for the block will be stored in lastKey.
    size_t finish(std::vector<char> & out, CellKey & lastKey)
    {
        builder.finalize();
        out.resize(builder.getFinalSize());
        builder.exportTo(&out[0]);

        typedef kdi::marshal::CellBlock CellBlock;
        typedef kdi::marshal::CellData CellData;

        CellBlock const * block =
            reinterpret_cast<CellBlock const *>(&out[0]);
        if(!block->cells.empty())
        {
            CellData const * c = block->cells.end() - 1;
            lastKey.setRow(*c->key.row);
            lastKey.setColumn(*c->key.column);
            lastKey.setTimestamp(c->key.timestamp);
        }
        return block->cells.size();
    }

    size_t getCellCount() const
    {
        return cells.getCellCount();
    }

    size_t getDataSize() const
    {
        return cells.getDataSize();
    }
};

#endif // KDI_SERVER_CELLBUILDER_H
