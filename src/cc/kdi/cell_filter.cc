//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-27
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

#include <kdi/cell_filter.h>

#include <flux/filter.h>
#include <ex/exception.h>
#include <kdi/scan_predicate.h>
#include <algorithm>

using namespace kdi;
using namespace ex;

//----------------------------------------------------------------------------
// ContainmentFilter
//----------------------------------------------------------------------------
namespace
{
    /// Functor that returns true iff a derived key is contained
    /// in a container.  The functor holds a (hopefully smart) pointer
    /// to the container.
    /// @param CPtr Container pointer type.  The expression
    /// ptr->contains(x) must be well defined, where ptr is of type
    /// CPtr, and x is the type returned by the key functor.
    /// @param K Key functor.  Must map the predicate argument to the
    /// value to check for in the Container.
    template <class CPtr, class K>
    class ContainmentPredicate
    {
        CPtr ptr;
        K getKey;

    public:
        explicit ContainmentPredicate(CPtr const & ptr,
                                      K const & getKey=K()) :
            ptr(ptr), getKey(getKey)
        {
            using namespace ex;
            if(!ptr)
                raise<ValueError>("null container pointer");
        }

        template <class T>
        bool operator()(T const & x) const
        {
            return ptr->contains(getKey(x));
        }
    };

    /// Helper function to make a flux::Filter stream using a
    /// ContainmentPredicate.
    template <class T, class CPtr, class K>
    typename flux::Stream<T>::handle_t makeContainmentFilter(
        CPtr const & ptr, K const & getKey)
    {
        return flux::makeFilter<T>(
            ContainmentPredicate<CPtr,K>(ptr, getKey)
            );
    }

    /// Functor to get the row name from a Cell.
    struct GetCellRow
    {
        std::string operator()(Cell const & x) const
        {
            return warp::str(x.getRow());
        }
    };

    /// Functor to get the column name from a Cell.
    struct GetCellColumn
    {
        std::string operator()(Cell const & x) const
        {
            return warp::str(x.getColumn());
        }
    };

    /// Functor to get the timestamp value from a Cell.
    struct GetCellTimestamp
    {
        int64_t operator()(Cell const & x) const
        {
            return x.getTimestamp();
        }
    };
}

//----------------------------------------------------------------------------
// HistoryFilter
//----------------------------------------------------------------------------
namespace
{
    class HistoryFilter : public CellStream
    {
        CellStreamPtr input;
        Cell last;
        size_t currentHistoryLength;
        size_t initialHistoryLength;
        size_t maxHistoryLength;

    public:
        explicit HistoryFilter(int k)
        {
            if(k > 0)
            {
                initialHistoryLength = 0;
                maxHistoryLength = k;
            }
            else
            {
                initialHistoryLength = size_t(k) - 1;
                maxHistoryLength = size_t(k) - 1;
            }
        }

        void pipeFrom(CellStreamPtr const & input)
        {
            this->input = input;
            last.release();
        }

        bool get(Cell & x)
        {
            if(!input)
                return false;

            for(;;)
            {
                // Get a new cell
                if(!input->get(x))
                    return false;

                // See if it starts a new history sequence
                if(!last ||
                   last.getRow() != x.getRow() ||
                   last.getColumn() != x.getColumn())
                {
                    // New history sequence
                    currentHistoryLength = initialHistoryLength;
                }

                // Update history length and remember last element
                ++currentHistoryLength;
                last = x;

                // Return if we're in the window
                if(currentHistoryLength <= maxHistoryLength)
                    return true;
            }
        }
    };
}

//----------------------------------------------------------------------------
// ErasureFilter
//----------------------------------------------------------------------------
namespace
{
    class ErasureFilter : public CellStream
    {
        CellStreamPtr input;

    public:
        void pipeFrom(CellStreamPtr const & input)
        {
            this->input = input;
        }

        bool get(Cell & x)
        {
            if(!input)
                return false;

            // Find the next non-erasure cell
            while(input->get(x))
            {
                if(!x.isErasure())
                    return true;
            }

            // Out of cells
            return false;
        }
    };
}

//----------------------------------------------------------------------------
// Filter functions
//----------------------------------------------------------------------------
CellStreamPtr kdi::applyPredicateFilter(ScanPredicate const & pred,
                                        CellStreamPtr const & input)
{
    EX_CHECK_NULL(input);
    CellStreamPtr stream = input;

    // Install row filter if predicate specifies a row constraint
    if(pred.getRowPredicate())
    {
        CellStreamPtr filter = makeRowFilter(pred.getRowPredicate());
        filter->pipeFrom(stream);
        std::swap(filter,stream);
    }

    // Install column filter if predicate specifies a column
    // constraint
    if(pred.getColumnPredicate())
    {
        CellStreamPtr filter = makeColumnFilter(pred.getColumnPredicate());
        filter->pipeFrom(stream);
        std::swap(filter,stream);
    }
    
    // Install timestamp filter if predicate specifies a timestamp
    // constraint
    if(pred.getTimePredicate())
    {
        CellStreamPtr filter = makeTimestampFilter(pred.getTimePredicate());
        filter->pipeFrom(stream);
        std::swap(filter,stream);
    }

    // Install history filter if predicate specifies a history
    // constraint
    if(pred.getMaxHistory())
    {
        CellStreamPtr filter = makeHistoryFilter(pred.getMaxHistory());
        filter->pipeFrom(stream);
        std::swap(filter,stream);
    }
    
    return stream;
}

CellStreamPtr kdi::makeRowFilter(
    ScanPredicate::StringSetCPtr const & keepSet)
{
    return makeContainmentFilter<Cell>(keepSet, GetCellRow());
}

CellStreamPtr kdi::makeColumnFilter(
    ScanPredicate::StringSetCPtr const & keepSet)
{
    return makeContainmentFilter<Cell>(keepSet, GetCellColumn());
}

CellStreamPtr kdi::makeTimestampFilter(
    ScanPredicate::TimestampSetCPtr const & keepSet)
{
    return makeContainmentFilter<Cell>(keepSet, GetCellTimestamp());
}

CellStreamPtr kdi::makeHistoryFilter(int maxHistory)
{
    CellStreamPtr p(new HistoryFilter(maxHistory));
    return p;
}

CellStreamPtr kdi::makeErasureFilter()
{
    CellStreamPtr p(new ErasureFilter);
    return p;
}
