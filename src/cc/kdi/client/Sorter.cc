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

#include <kdi/client/Sorter.h>
#include <kdi/rpc/PackedCellReader.h>
#include <algorithm>
#include <cassert>

using namespace kdi;
using namespace kdi::client;

using kdi::rpc::PackedCellReader;
using kdi::rpc::PackedCellWriter;

//----------------------------------------------------------------------------
// Sorter
//----------------------------------------------------------------------------
void Sorter::load(PackedCellWriter const & unsorted)
{
    cells.clear();
    idx.clear();

    size_t n = unsorted.getCellCount();
    cells.resize(n);
    idx.resize(n);

    PackedCellReader reader(unsorted.getPacked());
    for(size_t i = 0; i < n; ++i)
    {
        if(!reader.next())
            assert(false);

        CellRef * c = &cells[i];
        idx[i] = c;

        reader.getKey(c->key);
        c->erasure = reader.isErasure();
        if(!c->erasure)
            c->value = reader.getValue();
    }
}

void Sorter::writeSorted()
{
    sorted.reset();

    // Sort the cells (use a stable sort since ordering between
    // duplicates is important)
    std::stable_sort(idx.begin(), idx.end(), CellRefLt());

    // Unique the cells.  We want the last cell of each duplicate
    // sequence instead of the first, so we'll unique using reverse
    // iterators.  The end returned by unique will be the first cell
    // to keep in the forward iterator world.
    std::vector<CellRef *>::iterator i =
        std::unique(idx.rbegin(), idx.rend(), CellRefEq()).base();

    // Write cells to output
    for(; i != idx.end(); ++i)
    {
        if(!(*i)->erasure)
            sorted.append((*i)->key.getRow(),
                          (*i)->key.getColumn(),
                          (*i)->key.getTimestamp(),
                          (*i)->value);
        else
            sorted.appendErasure((*i)->key.getRow(),
                                 (*i)->key.getColumn(),
                                 (*i)->key.getTimestamp());
    }

    sorted.finish();
}
