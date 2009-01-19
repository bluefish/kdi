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

#include <kdi/local/disk_table.h>
#include <kdi/local/table_types.h>
#include <kdi/scan_predicate.h>
#include <kdi/cell_filter.h>
#include <oort/fileio.h>
#include <warp/file.h>
#include <warp/adler.h>
#include <warp/bloom_filter.h>
#include <warp/log.h>
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
                        IndexEntryV1 const & b) const
        {
            return lt(a, *b.lastRow);
        }

        bool operator()(IndexEntryV1 const & a,
                        IntervalPoint<string> const & b) const
        {
            return lt(*a.lastRow, b);
        }
    };
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

        // Column and timestamp ranges for filtering blocks
        boost::shared_ptr< vector<string> > columnFamilies;
        uint32_t colFamilyMask;
        ScanPredicate::TimestampSetCPtr times;

        // Index data
        CacheRecord indexRec;
        IndexEntryV1 const * indexIt;

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
        bool readNextBlock(IndexEntryV1 const *nextIndexIt = 0)
        {
            nextBlock:

            // Advance to the index entry for the next block
            if(nextIndexIt) {
                if(nextIndexIt >= indexRec.cast<BlockIndexV1>()->blocks.end()) return false;
                input->seek(nextIndexIt->blockOffset);
                indexIt = nextIndexIt;
                nextIndexIt = 0;
            } else if(indexIt) {
                ++indexIt;
            } else {
                indexIt = indexRec.cast<BlockIndexV1>()->blocks.begin();
            }

            if(times) {
                IntervalPoint<int64_t> lowTime(indexIt->lowestTime, PT_INCLUSIVE_LOWER_BOUND);
                IntervalPoint<int64_t> highTime(indexIt->highestTime, PT_INCLUSIVE_UPPER_BOUND);
                Interval<int64_t> timeInterval(lowTime, highTime);

                // If there is no overlap between the time ranges we are looking for and the
                // interval of times in this block, skip to the next block
                if(!times->overlaps(timeInterval)) {
                    nextIndexIt = indexIt+1;
                    goto nextBlock;
                }
            }

            if(colFamilyMask) {
                if(!(colFamilyMask & indexIt->colFamilyMask)) {
                    nextIndexIt = indexIt+1;
                    goto nextBlock;
                }
            }

            // Read the next record and make sure it is a CellBlock
            if(input->get(blockRec) && blockRec.tryAs<CellBlock>())
            {
                // Verify the checksum
                uint32_t checksum = adler((uint8_t*)blockRec.getData(), blockRec.getLength());
                if(indexIt->blockChecksum != checksum) {
                    log("BAD CHECKSUM: skipping block");
                    goto nextBlock;
                }

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

            // If we're at the last row, there is no next row segment, we're done.
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
            BlockIndexV1 const * index = indexRec.cast<BlockIndexV1>();
            IndexEntryV1 const * ent;

            ent = std::lower_bound(
                index->blocks.begin(), index->blocks.end(),
                *lowerBoundIt, RowLt());

            if(ent == index->blocks.end())
                return false;

            // Seek to the next block position and load the block.
            if(!readNextBlock(ent))
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
        explicit DiskScanner(FilePtr const & fp,
                             IndexCache * cache,
                             string const & fn) :
            input(FileInput::make(fp)),
            upperBound(string(), PT_INFINITE_UPPER_BOUND),
            colFamilyMask(0),
            indexRec(cache, fn),
            indexIt(0),
            cellIt(0),
            cellEnd(0)
        {
        }

        /// Create a DiskScanner over a set of rows
        DiskScanner(FilePtr const & fp,
                    ScanPredicate::StringSetCPtr const & rows,
                    boost::shared_ptr< vector<string> > const & columnFamilies,
                    ScanPredicate::TimestampSetCPtr const & times,
                    IndexCache * cache,
                    string const & fn) :
            input(FileInput::make(fp)),
            rows(rows),
            upperBound(string(), PT_INFINITE_UPPER_BOUND),
            columnFamilies(columnFamilies),
            colFamilyMask(0),
            times(times),
            indexRec(cache, fn),
            indexIt(0),
            cellIt(0),
            cellEnd(0)
        {
            if(rows)
                nextRowIt = rows->begin();

            if(columnFamilies) {
                BlockIndexV1 const * index = indexRec.as<BlockIndexV1>();

                // Figure out the column family bitmask now
                vector<string>::const_iterator cfi;
                for(cfi = columnFamilies->begin(); cfi != columnFamilies->end(); ++cfi) {
                    uint32_t nextMask = 1;
                    warp::StringOffset const * si;
                    for(si = index->colFamilies.begin(); si != index->colFamilies.end(); ++si) {
                        if(**si == *cfi) {
                            colFamilyMask |= nextMask;
                            break;
                        }
                        nextMask = nextMask << 1;
                    }
                }
            }
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

        IndexEntryV1 const * cur;
        IndexEntryV1 const * end;
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
            BlockIndexV1 const * index = indexRec.as<BlockIndexV1>();

            RowLt lt;

            cur = std::lower_bound(
                index->blocks.begin(), index->blocks.end(),
                rows.getLowerBound(), lt);

            end = std::lower_bound(
                cur, index->blocks.end(),
                rows.getUpperBound(), lt);

            if(end != index->blocks.end())
                ++end;

            if(cur != index->blocks.begin())
                base = cur[-1].blockOffset;
        }

        bool get(pair<string, size_t> & x)
        {
            if(cur == end)
                return false;

            x.first.assign(cur->lastRow->begin(), cur->lastRow->end());
            x.second = cur->blockOffset - base;
            base = cur->blockOffset;
            ++cur;

            return true;
        }
    };
}

//------------------------------------------------------------------------------
// DiskTable - Always immutable
//------------------------------------------------------------------------------
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

size_t DiskTable::readVersion(std::string const & fn)
{
    // Open file to TableInfo record
    FilePtr fp = File::input(fn);
    fp->seek(-(10+sizeof(TableInfo)), SEEK_END);

    // Make a Record reader
    FileInput::handle_t input = FileInput::make(fp);

    // Read TableInfo
    oort::Record r;
    if(!input->get(r) || !r.getType() == TableInfo::TYPECODE)
        raise<RuntimeError>("could not read TableInfo record: %s", fn);

    return r.getVersion();
}

DiskTablePtr DiskTable::loadTable(std::string const & fn)
{
    size_t version = readVersion(fn);
    switch(version)
    {
        case 0: return DiskTablePtr(new DiskTableV0(fn));
        case 1: return DiskTablePtr(new DiskTableV1(fn));
    }

    raise<RuntimeError>("Unknown TableInfo version %d: %s", version, fn);
}

off_t DiskTable::loadIndex(std::string const & fn, oort::Record & r)
{
    // Open file to TableInfo record
    FilePtr fp = File::input(fn);
    fp->seek(-(10+sizeof(TableInfo)), SEEK_END);

    // Make a Record reader
    FileInput::handle_t input = FileInput::make(fp);

    // Read TableInfo
    if(!input->get(r) || r.getType() != TableInfo::TYPECODE)
        raise<RuntimeError>("could not read TableInfo record: %s", fn);

    // Seek to BlockIndex
    uint64_t indexOffset = r.cast<TableInfo>()->indexOffset;
    input->seek(indexOffset);

    // Read BlockIndex
    if(!input->get(r) || r.getType() != BlockIndex::TYPECODE)
        raise<RuntimeError>("could not read BlockIndex record: %s", fn);

    return indexOffset;
}

//----------------------------------------------------------------------------
// DiskTableV1
//----------------------------------------------------------------------------
DiskTableV1::DiskTableV1(string const & fn) :
    cache(IndexCache::getGlobal()), fn(fn), indexSize(0), dataSize(0)
{
    oort::Record r;
    dataSize = loadIndex(fn, r);
    indexSize = r.getLength();

    // Make sure we have the right type
    r.as<BlockIndexV1>();
}

DiskTableV1::~DiskTableV1()
{
    cache->remove(fn);
}

CellStreamPtr DiskTableV1::scan(ScanPredicate const & pred) const
{
    FilePtr fp = File::input(fn);

    // Make a disk scanner appropriate for our row predicate
    CellStreamPtr diskScanner;
    if(ScanPredicate::StringSetCPtr const & rows = pred.getRowPredicate())
    {
        //ScanPredicate::StringSetCPtr const & columns = pred.getColumnPredicate();
        ScanPredicate::TimestampSetCPtr const & times = pred.getTimePredicate();

        boost::shared_ptr< vector<string> > famsCopy;
        vector<StringRange> fams;
        if(pred.getColumnFamilies(fams))
        {
            // Make a copy so the families don't get invalidated after
            // the predicate goes out of scope.
            famsCopy.reset(new vector<string>(fams.size()));
            for(size_t i = 0; i < fams.size(); ++i)
            {
                (*famsCopy)[i] = fams[i].toString();
            }
        }

        // Make a scanner that handles the row predicate
        diskScanner.reset(new DiskScanner(fp, rows, famsCopy, times, cache, fn));
    }
    else
    {
        // Scan everything
        diskScanner.reset(new DiskScanner(fp, cache, fn));
    }

    // Filter the rest
    return applyPredicateFilter(
        ScanPredicate(pred).clearRowPredicate(),
        diskScanner);
}

flux::Stream< std::pair<std::string, size_t> >::handle_t
DiskTableV1::scanIndex(warp::Interval<std::string> const & rows) const
{
    flux::Stream< std::pair<std::string, size_t> >::handle_t p(
        new IndexScanner(cache, fn, rows)
        );
    return p;
}
