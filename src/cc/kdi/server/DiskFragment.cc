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

#include <kdi/server/Fragment.h>
#include <kdi/server/DiskFragment.h>
#include <kdi/server/CellOutput.h>
#include <kdi/server/RestrictedFragment.h>
#include <kdi/scan_predicate.h>
#include <kdi/marshal/cell_block.h>
#include <kdi/local/index_cache.h>
#include <kdi/local/table_types.h>
#include <kdi/CellKey.h>
#include <oort/fileio.h>
#include <warp/file.h>
#include <warp/fs.h>
#include <warp/functional.h>
#include <warp/adler.h>
#include <warp/bloom_filter.h>
#include <warp/log.h>
#include <warp/util.h>
#include <ex/exception.h>

using namespace std;
using namespace ex;
using namespace warp;
using namespace oort;
using namespace kdi;
using namespace kdi::server;

using kdi::marshal::CellData;
using kdi::marshal::CellBlock;
using kdi::local::IndexCache;
using kdi::local::CacheRecord;
using kdi::local::disk::IndexEntryV1;
using kdi::local::disk::BlockIndexV1;

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

    struct KeyLt
    {
        bool operator()(CellData const & a, CellKey const & b)
        {
            return warp::order(
                static_cast<strref_t>(*a.key.row), 
                static_cast<strref_t>(b.getRow()),
                static_cast<strref_t>(*a.key.column), 
                static_cast<strref_t>(b.getColumn()),
                a.key.timestamp, b.getTimestamp());
        }
    };

    bool operator<(kdi::marshal::CellData const & a, kdi::CellKey const & b)
    {
        return warp::order(
            static_cast<strref_t>(*a.key.row), static_cast<strref_t>(b.getRow()),
            static_cast<strref_t>(*a.key.column), static_cast<strref_t>(b.getColumn()),
            a.key.timestamp, b.getTimestamp());
    }
}

//----------------------------------------------------------------------------
// DiskBlockReader
//----------------------------------------------------------------------------
class kdi::server::DiskBlockReader
    : public kdi::server::FragmentBlockReader
{
    Record blockRec;
    ScanPredicate pred;

    CellBlock const * block;
    CellData const * cellIt;
    CellData const * cellEnd;

    ScanPredicate::StringSetCPtr rows;
    ScanPredicate::StringSetCPtr cols;
    ScanPredicate::TimestampSetCPtr times;

    IntervalSet<string>::const_iterator nextRowIt;
    
    bool getMoreCells();

public:
    DiskBlockReader(Record const & r, ScanPredicate const & pred);
    
    virtual bool advance(CellKey & nextKey);
    virtual void copyUntil(const CellKey * stopKey, CellOutput & out);
};

DiskBlockReader::DiskBlockReader(Record const & blockRec, ScanPredicate const & pred) :
    blockRec(blockRec),
    pred(pred),
    block(blockRec.cast<CellBlock>()),
    cellIt(0), cellEnd(0),
    rows(pred.getRowPredicate()),
    cols(pred.getColumnPredicate()),
    times(pred.getTimePredicate())
{
    if(rows) nextRowIt = rows->begin();
}

bool DiskBlockReader::getMoreCells()
{
    if(!rows) 
    {
        if(!cellIt) 
        {
            cellIt = block->cells.begin();
            cellEnd = block->cells.end();
            return true;
        }
        return false;
    }

    while(cellIt == cellEnd) {
        if(nextRowIt == rows->end()) return false;
        if(cellIt == block->cells.end()) return false;

        // Get the next row range and advance row iterator
        IntervalSet<string>::const_iterator lowerBoundIt = nextRowIt;
        IntervalPoint<string> upperBound;

        ++nextRowIt;
        assert(nextRowIt != rows->end());
        upperBound = *nextRowIt;
        ++nextRowIt;

        CellData const * cell = std::lower_bound(
            block->cells.begin(), block->cells.end(),
            *lowerBoundIt, RowLt());

        if(cell == block->cells.end()) return false;

        // Move the cell iterator and upper bound forward
        cellIt = cell;
        if(upperBound.isInfinite()) {
            cellEnd = block->cells.end();
        } else {
            cellEnd = std::upper_bound(
                cellIt, block->cells.end(), 
                upperBound, RowLt());
        }
    }

    return true;
}

bool DiskBlockReader::advance(CellKey & nextKey)
{
    if(cellIt == cellEnd) 
    {
        if(!getMoreCells()) return false;
    }
    else
    {
        if(++cellIt == cellEnd && !getMoreCells()) return false; 
    }

    // skip cells that don't match the predicate
    while((times && !times->contains(cellIt->key.timestamp)) ||
          (cols && !cols->contains(*cellIt->key.column)))
    {
        if(++cellIt == cellEnd && !getMoreCells()) return false; 
    }

    nextKey.setRow(*cellIt->key.row);
    nextKey.setColumn(*cellIt->key.column);
    nextKey.setTimestamp(cellIt->key.timestamp);
    return true;
}

void DiskBlockReader::copyUntil(CellKey const * stopKey, CellOutput & out)
{
    if(stopKey && cellIt != cellEnd && !(*cellIt < *stopKey)) return;

    CellData const * stopIt = block->cells.end();

    if(stopKey)
    {
        stopIt = std::lower_bound(cellIt, block->cells.end(), *stopKey, KeyLt()); 
    }

    while(cellIt < stopIt)
    {
        while(cellIt != stopIt && cellIt != cellEnd)
        {
            // Filter cells not matching the predicate
            if((!times || times->contains(cellIt->key.timestamp)) &&
               (!cols || cols->contains(*cellIt->key.column))) 
            {
                if(cellIt->value) 
                {
                    out.emitCell(*cellIt->key.row, *cellIt->key.column,
                                 cellIt->key.timestamp, *cellIt->value);
                } else {
                    out.emitErasure(*cellIt->key.row, *cellIt->key.column,
                                    cellIt->key.timestamp);
                }
            }

            ++cellIt;
        }

        if(cellIt == cellEnd && !getMoreCells()) return;
    }

    --cellIt;
}

//----------------------------------------------------------------------------
// DiskBlock
//----------------------------------------------------------------------------
DiskBlock::DiskBlock(FilePtr const & fp, IndexEntryV1 const & idx)
{
    FileInput::handle_t input = FileInput::make(fp);
    input->seek(idx.blockOffset);
    input->get(blockRec);
}

DiskBlock::~DiskBlock()
{
    blockRec.release();
}

std::auto_ptr<FragmentBlockReader> 
DiskBlock::makeReader(ScanPredicate const & pred) const 
{
    return std::auto_ptr<FragmentBlockReader>(
        new DiskBlockReader(blockRec, pred)); 
}

//----------------------------------------------------------------------------
// DiskFragment
//----------------------------------------------------------------------------
DiskFragment::DiskFragment(std::string const & filename) :
    filename(filename),
    indexRec(IndexCache::getGlobal(), filename),
    dataSize(fs::filesize(filename))
{
}

DiskFragment::DiskFragment(std::string const & loadPath,
                           std::string const & filename) :
    filename(filename),
    indexRec(IndexCache::getGlobal(), loadPath),
    dataSize(fs::filesize(loadPath))
{
}

std::string DiskFragment::getFilename() const
{
    return filename;
}

void DiskFragment::getColumnFamilies(
    std::vector<std::string> & families) const
{
    families.clear();
    BlockIndexV1 const * index = indexRec.cast<BlockIndexV1>();
    for(warp::StringOffset const * i = index->colFamilies.begin();
        i != index->colFamilies.end(); ++i)
    {
        families.push_back((*i)->toString());
    }
}

FragmentCPtr DiskFragment::getRestricted(
    std::vector<std::string> const & families) const
{
    return makeRestrictedFragment(shared_from_this(), families);
}

size_t DiskFragment::nextBlock(ScanPredicate const & pred, size_t minBlock) const
{
    BlockIndexV1 const * index = indexRec.cast<BlockIndexV1>();

    uint32_t colFamilyMask = 0;
    std::vector<warp::StringRange> families;
    if(pred.getColumnFamilies(families))
    {
        vector<warp::StringRange>::const_iterator cfi;
        for(cfi = families.begin(); cfi != families.end(); ++cfi) {
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

nextBlock:

    if(minBlock >= index->blocks.size())
        return size_t(-1);

    ScanPredicate::StringSetCPtr rows = pred.getRowPredicate();
    ScanPredicate::TimestampSetCPtr times = pred.getTimePredicate();

    IndexEntryV1 const * ent = &index->blocks[minBlock];

    if(rows)
    {
        // This should really use std::lower_bound as well, not linear search,
        // but much less important here
        IntervalSet<string>::const_iterator lowerBoundIt = rows->begin();
        while(lowerBoundIt != rows->end())
        {
            IntervalSet<string>::const_iterator upperBoundIt = lowerBoundIt+1;
            if(!RowLt()(*upperBoundIt, index->blocks[minBlock])) break;
            lowerBoundIt = upperBoundIt+1;
        }
        if(lowerBoundIt == rows->end()) return size_t(-1);

        // Try finding the next block based on the index
        ent = std::lower_bound(
                &index->blocks[minBlock], index->blocks.end(),
                *lowerBoundIt, RowLt());

        if(ent == index->blocks.end())
            return size_t(-1);

        minBlock = ent-index->blocks.begin();
    } 

    if(times) {
        IntervalPoint<int64_t> lowTime(ent->lowestTime, PT_INCLUSIVE_LOWER_BOUND);
        IntervalPoint<int64_t> highTime(ent->highestTime, PT_INCLUSIVE_UPPER_BOUND);
        Interval<int64_t> timeInterval(lowTime, highTime);

        // If there is no overlap between the time ranges we are looking for and the
        // interval of times in this block, skip to the next block
        if(!times->overlaps(timeInterval)) {
            ++minBlock;
            goto nextBlock;
        }
    }

    if(colFamilyMask) {
        if(!(colFamilyMask & ent->colFamilyMask)) {
            ++minBlock;
            goto nextBlock;
        }
    }

    return minBlock;
}

std::auto_ptr<FragmentBlock> DiskFragment::loadBlock(size_t blockAddr) const 
{
    warp::FilePtr fp(File::input(filename));
    BlockIndexV1 const * index = indexRec.cast<BlockIndexV1>();
    IndexEntryV1 const & idx = index->blocks[blockAddr];
    return std::auto_ptr<FragmentBlock>(new DiskBlock(fp, idx));
}

/*
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

*/
