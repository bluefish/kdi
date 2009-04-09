//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-06
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

#ifndef KDI_CLIENT_SORTER_H
#define KDI_CLIENT_SORTER_H

#include <kdi/rpc/PackedCellWriter.h>
#include <kdi/CellKey.h>
#include <warp/string_range.h>
#include <vector>

namespace kdi {
namespace client {

    class Sorter;

} // namespace client
} // namespace kdi

//----------------------------------------------------------------------------
// Sorter
//----------------------------------------------------------------------------
class kdi::client::Sorter
{
public:
    /// Copy data from an unsorted writer into a new writer, in proper
    /// cell order.  Returns a reference to the packed result, which
    /// is only valid until the next call to reorder (or the end of
    /// the Sorter object).
    warp::StringRange
    reorder(kdi::rpc::PackedCellWriter const & unsorted)
    {
        load(unsorted);
        writeSorted();
        return sorted.getPacked();
    }

private:
    struct CellRef
    {
        CellKeyRef key;
        warp::StringRange value;
        bool erasure;
    };

    struct CellRefLt
    {
        bool operator()(CellRef const * a, CellRef const * b) const
        {
            return a->key < b->key;
        }
    };

    struct CellRefEq
    {
        bool operator()(CellRef const * a, CellRef const * b) const
        {
            return a->key == b->key;
        }
    };

private:
    /// Load data from another cell writer.  The writer should
    /// be finalized.
    void load(kdi::rpc::PackedCellWriter const & unsorted);

    /// Sort and unique cell index, then write results into sorted
    /// buffer.
    void writeSorted();

private:
    std::vector<CellRef> cells;
    std::vector<CellRef *> idx;
    kdi::rpc::PackedCellWriter sorted;
};


#endif // KDI_CLIENT_SORTER_H
