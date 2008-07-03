//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-12
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef KDI_CELL_MERGE_H
#define KDI_CELL_MERGE_H

#include <flux/merge.h>
#include <kdi/cell.h>

namespace kdi {

    /// Cell stream operator to do an unique ordered merge of multiple
    /// input Cell streams.  Erasure Cells can be filtered out of the
    /// resulting stream if desired.
    class CellMerge;

} // namespace kdi

//----------------------------------------------------------------------------
// CellMerge
//----------------------------------------------------------------------------
class kdi::CellMerge : public flux::Merge<kdi::Cell>
{
    typedef flux::Merge<kdi::Cell> super;
    bool filterErasures;

public:
    explicit CellMerge(bool filterErasures) :
        super(true, super::value_cmp_t()),
        filterErasures(filterErasures)
    {
    }

    bool get(Cell & x)
    {
        if(filterErasures)
        {
            // Only return non-erasure cells
            while(super::get(x))
            {
                if(!x.isErasure())
                    return true;
            }
            // No more cells
            return false;
        }
        else
        {
            // Return all merged cells
            return super::get(x);
        }
    }

    /// Return a smart pointer to a new CellMerge stream
    static boost::shared_ptr<CellMerge> make(bool filterErasures)
    {
        boost::shared_ptr<CellMerge> p(new CellMerge(filterErasures));
        return p;
    }
};

#endif // KDI_CELL_MERGE_H
