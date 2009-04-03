//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-02-27
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

#ifndef KDI_SERVER_CELLBUILDER_H
#define KDI_SERVER_CELLBUILDER_H

#include <kdi/server/CellOutput.h>
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
    : public CellOutput
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

    void emitCell(strref_t row, strref_t column, int64_t timestamp, strref_t value)
    {
        cells.appendCell(row, column, timestamp, value);
    }

    void emitErasure(strref_t row, strref_t column, int64_t timestamp)
    {
        cells.appendErasure(row, column, timestamp);
    }
};

#endif // KDI_SERVER_CELLBUILDER_H
