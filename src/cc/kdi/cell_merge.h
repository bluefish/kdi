//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/cell_merge.h#1 $
//
// Created 2007/10/12
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
