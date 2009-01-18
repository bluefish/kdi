//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-04
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

/*
 * DEPRECATED
 *
 * This file contains the original disk table implementation for backwords
 * compatibility.  The writer for this disk table format has been removed as
 * obsolete.  This implementation can be removed only when there are no 
 * more users of the original disk table format.
 */

#include <kdi/local/disk_table.h>
#include <kdi/local/table_types.h>
#include <kdi/scan_predicate.h>
#include <kdi/cell_filter.h>
#include <oort/fileio.h>
#include <warp/file.h>
#include <ex/exception.h>

using namespace std;
using namespace ex;
using namespace warp;
using namespace oort;
using namespace kdi;
using namespace kdi::local;
using namespace kdi::local::disk; 

namespace 
{
    struct RowLt
    {
        IntervalPointOrder<warp::less> lt;

        bool operator()(IntervalPoint<string> const & a,
                        CellData const & b) const
        {
            return lt(a, *b.key.row);
        }

        bool operator()(CellData const & a,
                        IntervalPoint<string> const & b) const
        {
            return lt(*a.key.row, b);
        }

        bool operator()(IntervalPoint<string> const & a,
                        IndexEntryV0 const & b) const
        {
            return lt(a, *b.startKey.row);
        }

        bool operator()(IndexEntryV0 const & a,
                        IntervalPoint<string> const & b) const
        {
            return lt(*a.startKey.row, b);
        }
    };


    IndexEntryV0 const * findIndexEntry(BlockIndexV0 const * index,
                                      IntervalPoint<string> const & beginRow)
    {
        IndexEntryV0 const * ent = std::lower_bound(
            index->blocks.begin(), index->blocks.end(),
            beginRow, RowLt());

        // Really should have kept track of end row...
        if(ent != index->blocks.begin())
            --ent;

        return ent;
    }
}

//----------------------------------------------------------------------------
// DiskScanner
//----------------------------------------------------------------------------
namespace 
{
    class DiskScannerV0 : public CellStream
    {
        FileInput::handle_t input;

        // Row range data for scans with predicates
        ScanPredicate::StringSetCPtr rows;
        IntervalSet<string>::const_iterator nextRowIt;
        IntervalPoint<string> upperBound;
        CacheRecord indexRec;

        // Current CellBlock
        Record blockRec;
        CellData const * cellIt;
        CellData const * cellEnd;

        /// Read the next CellBlock record from the input stream.  If
        /// there is a block, set the cell iterators to the full range
        /// of the block.  Otherwise, release the record and set the
        /// cell iterators to an empty range.
        /// @returns true if a block was read, false if no block was
        /// available
        bool readNextBlock()
        {
            // Read the next record and make sure it is a CellBlock
            if(input->get(blockRec) && blockRec.tryAs<CellBlock>())
            {
                // Got one -- reset Cell iterators
                CellBlock const * block = blockRec.cast<CellBlock>();
                cellIt = block->cells.begin();
                cellEnd = block->cells.end();
                return true;
            }
            else
            {
                // Nothing left
                cellIt = cellEnd = 0;
                blockRec.release();
                return false;
            }
        }

        /// Load the next row segment.  The cell block containing the
        /// beginning of the next row segment is loaded and the
        /// current cell iterator is set to point to the first cell
        /// contained in the row range lower bound.  The end cell
        /// iterator is set to the end of the block, regardless of
        /// whether or not it is contained in the row range.  The end
        /// row variables are set to indicate the end of the range.
        /// @return true if the next segment was loaded, or false for
        /// end of stream
        bool getNextRowSegment()
        {
            // If we have no row set, then we're reading everything.
            // The first time we get here we'll read the first block
            // of the file.  The next time we come back, we'll be at
            // the end of the file.
            if(!rows)
                return readNextBlock();

            // If we're at the last rowthere is no next row segment, we're done.
            if(nextRowIt == rows->end())
                return false;

            // Get the next row range and advance row iterator
            IntervalSet<string>::const_iterator lowerBoundIt = nextRowIt;
            ++nextRowIt;
            assert(nextRowIt != rows->end());
            upperBound = *nextRowIt;
            ++nextRowIt;

            // If the current block contains the beginning of the
            // next row segment, use it.
            if(blockRec)
            {
                CellBlock const * block = blockRec.cast<CellBlock>();
                CellData const * cell = std::lower_bound(
                    block->cells.begin(), block->cells.end(),
                    *lowerBoundIt, RowLt());
                if(cell != block->cells.end())
                {
                    cellIt = cell;
                    cellEnd = block->cells.end();
                    return true;
                }
            }

            // Else use the index to find the position of the next
            // row segment.  If the index points us off the end,
            // we're done.
            BlockIndexV0 const * index = indexRec.cast<BlockIndexV0>();
            IndexEntryV0 const * ent = findIndexEntry(index, *lowerBoundIt);
            if(ent == index->blocks.end())
                return false;

            // Seek to the next block position and load the block.
            input->seek(ent->blockOffset);
            if(!readNextBlock())
                return false;

            // Set the start cell iterator
            CellBlock const * block = blockRec.cast<CellBlock>();
            cellIt = std::lower_bound(
                block->cells.begin(), block->cells.end(),
                *lowerBoundIt, RowLt());
            return true;
        }

        /// Update the cell iterators to point to the next range of
        /// cells.
        /// @returns true if another range is available (note the
        /// range may be empty)
        bool getMoreCells()
        {
            // First, get the next CellBlock.

            // If we've scanned to the end of the current block
            // (instead of stopping in the middle somewhere), then the
            // current row segment might extend into the next block.
            if(blockRec && cellEnd == blockRec.cast<CellBlock>()->cells.end())
            {
                // Load the next block.  If there is no next block,
                // then we're at the end of the stream.
                if(!readNextBlock())
                    return false;
            }
            else
            {
                // Else we finished a row segment.  Find the block for
                // the next row segment.  If there is no next row
                // segment, we're done.
                if(!getNextRowSegment())
                    return false;
            }

            // Now we have a valid block, use the current end row to
            // get the last cell within the block.
            if(!upperBound.isInfinite())
            {
                cellEnd = std::upper_bound(
                    cellIt, cellEnd, upperBound, RowLt());
            }

            // We have more cells.
            return true;
        }

    public:
        /// Create a full-scan DiskScanner
        explicit DiskScannerV0(FilePtr const & fp) :
            input(FileInput::make(fp)),
            upperBound(string(), PT_INFINITE_UPPER_BOUND),
            cellIt(0),
            cellEnd(0)
        {
        }

        /// Create a DiskScanner over a set of rows
        DiskScannerV0(FilePtr const & fp,
                    ScanPredicate::StringSetCPtr const & rows,
                      IndexCache * cache, string const & fn) :
            input(FileInput::make(fp)),
            rows(rows),
            upperBound(string(), PT_INFINITE_UPPER_BOUND),
            indexRec(cache, fn),
            cellIt(0),
            cellEnd(0)
        {
            if(rows)
                nextRowIt = rows->begin();
        }

        bool get(Cell & x)
        {
            for(;;)
            {
                // If we still have stuff in the current cell range,
                // return the next one
                if(cellIt != cellEnd)
                {
                    if(cellIt->value)
                    {
                        // Normal Cell
                        x = makeCell(
                            *cellIt->key.row, *cellIt->key.column,
                            cellIt->key.timestamp, *cellIt->value);
                    }
                    else
                    {
                        // Null value means erasure Cell
                        x = makeCellErasure(
                            *cellIt->key.row, *cellIt->key.column,
                            cellIt->key.timestamp);
                    }
                    ++cellIt;
                    return true;
                }

                // Range is empty, get more cells
                if(!getMoreCells())
                    return false;
            }
        }
    };
}

//----------------------------------------------------------------------------
// IndexScanner
//----------------------------------------------------------------------------
namespace
{
    class IndexScanner
        : public flux::Stream< std::pair<std::string, size_t> >
    {
        CacheRecord indexRec;
        Interval<string> rows;

        IndexEntryV0 const * cur;
        IndexEntryV0 const * end;
        size_t base;

    public:
        IndexScanner(IndexCache * cache, string const & fn,
                     Interval<string> const & rows) :
            indexRec(cache, fn),
            rows(rows),
            cur(0),
            end(0),
            base(0)
        {
            BlockIndexV0 const * index = indexRec.as<BlockIndexV0>();
            
            cur = findIndexEntry(index, rows.getLowerBound());
            end = index->blocks.end();

            IntervalPointOrder<warp::less> lt;
            while(cur != end && !lt(rows.getLowerBound(), *cur->startKey.row))
            {
                base = cur->blockOffset;
                ++cur;
            }
        }

        bool get(pair<string, size_t> & x)
        {
            if(cur == end)
                return false;

            IntervalPointOrder<warp::less> lt;
            if(lt(rows.getUpperBound(), *cur->startKey.row))
            {
                end = cur;
                return false;
            }

            x.first.assign(cur->startKey.row->begin(), cur->startKey.row->end());
            x.second = cur->blockOffset - base;
            base = cur->blockOffset;
            ++cur;

            return true;
        }
    };
}


//----------------------------------------------------------------------------
// DiskTable
//----------------------------------------------------------------------------
DiskTableV0::DiskTableV0(string const & fn) :
    cache(IndexCache::get()), fn(fn), indexSize(0), dataSize(0)
{
    oort::Record r;
    dataSize = loadIndex(fn, r);
    indexSize = r.getLength();

    // Make sure we have the right type
    r.as<BlockIndexV0>();
}

DiskTableV0::~DiskTableV0()
{
    cache->remove(fn);
}

CellStreamPtr DiskTableV0::scan(ScanPredicate const & pred) const
{
    FilePtr fp = File::input(fn);

    // Make a disk scanner appropriate for our row predicate
    CellStreamPtr diskScanner;
    if(ScanPredicate::StringSetCPtr const & rows = pred.getRowPredicate())
    {
        // Make a scanner that handles the row predicate
        diskScanner.reset(new DiskScannerV0(fp, rows, cache, fn));
    }
    else
    {
        // Scan everything
        diskScanner.reset(new DiskScannerV0(fp));
    }

    // Filter the rest
    return applyPredicateFilter(
        ScanPredicate(pred).clearRowPredicate(),
        diskScanner);
}

flux::Stream< std::pair<std::string, size_t> >::handle_t
DiskTableV0::scanIndex(warp::Interval<std::string> const & rows) const
{
    flux::Stream< std::pair<std::string, size_t> >::handle_t p(
        new IndexScanner(cache, fn, rows)
        );
    return p;
}
