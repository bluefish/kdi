//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/local/disk_table.cc#2 $
//
// Created 2007/10/04
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

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
                        IndexEntry const & b) const
        {
            return lt(a, *b.startKey.row);
        }

        bool operator()(IndexEntry const & a,
                        IntervalPoint<string> const & b) const
        {
            return lt(*a.startKey.row, b);
        }
    };


    IndexEntry const * findIndexEntry(BlockIndex const * index,
                                      IntervalPoint<string> const & beginRow)
    {
        IndexEntry const * ent = std::lower_bound(
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
    class DiskScanner : public CellStream
    {
        FileInput::handle_t input;

        // Row range data for scans with predicates
        ScanPredicate::StringSetCPtr rows;
        IntervalSet<string>::const_iterator nextRowIt;
        IntervalPoint<string> upperBound;
        Record indexRec;

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
            BlockIndex const * index = indexRec.cast<BlockIndex>();
            IndexEntry const * ent = findIndexEntry(index, *lowerBoundIt);
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
        explicit DiskScanner(FilePtr const & fp) :
            input(FileInput::make(fp)),
            upperBound(string(), PT_INFINITE_UPPER_BOUND),
            cellIt(0),
            cellEnd(0)
        {
        }

        /// Create a DiskScanner over a set of rows
        DiskScanner(FilePtr const & fp,
                    ScanPredicate::StringSetCPtr const & rows,
                    Record const & indexRec) :
            input(FileInput::make(fp)),
            rows(rows),
            upperBound(string(), PT_INFINITE_UPPER_BOUND),
            indexRec(indexRec),
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
                            cellIt->key.row, cellIt->key.column,
                            cellIt->key.timestamp, cellIt->value);
                    }
                    else
                    {
                        // Null value means erasure Cell
                        x = makeCellErasure(
                            cellIt->key.row, cellIt->key.column,
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
// DiskTable
//----------------------------------------------------------------------------
DiskTable::DiskTable(string const & fn) :
    fn(fn)
{
    // Open file to TableInfo record
    FilePtr fp = File::input(fn);
    fp->seek(-(10+sizeof(TableInfo)), SEEK_END);

    // Make a Record reader
    FileInput::handle_t input = FileInput::make(fp);
    
    // Read TableInfo
    Record r;
    if(!input->get(r) || !r.tryAs<TableInfo>())
        raise<RuntimeError>("could not read TableInfo record: %s", fn);

    // Seek to BlockIndex
    uint64_t indexOffset = r.cast<TableInfo>()->indexOffset;
    input->seek(indexOffset);

    // Read BlockIndex
    if(!input->get(r) || !r.tryAs<BlockIndex>())
        raise<RuntimeError>("could not read BlockIndex record: %s", fn);

    // Make a copy of the index
    indexRec = r.clone();
}

void DiskTable::set(strref_t row, strref_t column, int64_t timestamp,
                    strref_t value)
{
    raise<NotImplementedError>("DiskTable::set() not implemented: "
                               "DiskTable is read-only");
}

void DiskTable::erase(strref_t row, strref_t column, int64_t timestamp)
{
    raise<NotImplementedError>("DiskTable::erase() not implemented: "
                               "DiskTable is read-only");
}

CellStreamPtr DiskTable::scan() const
{
    FilePtr fp = File::input(fn);
    CellStreamPtr h(new DiskScanner(fp));
    return h;
}

CellStreamPtr DiskTable::scan(ScanPredicate const & pred) const
{
    FilePtr fp = File::input(fn);

    // Make a scanner that handles the row predicate
    CellStreamPtr h(new DiskScanner(fp, pred.getRowPredicate(), indexRec));

    // Filter the rest
    return applyPredicateFilter(ScanPredicate(pred).clearRowPredicate(), h);
}
