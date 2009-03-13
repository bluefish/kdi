//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-05
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

#include <kdi/server/CellBuffer.h>
#include <kdi/server/errors.h>
#include <kdi/rpc/PackedCellReader.h>
#include <kdi/scan_predicate.h>

using namespace kdi;
using namespace kdi::server;
using kdi::rpc::PackedCellReader;

//----------------------------------------------------------------------------
// CellBuffer::BlockReader
//----------------------------------------------------------------------------
class CellBuffer::BlockReader
    : public FragmentBlockReader
{
    PackedCellReader reader;
    ScanPredicate pred;
    bool hitStop;

    bool moveToNext()
    {
        while(reader.next())
        {
            if(pred.getRowPredicate() &&
               !pred.getRowPredicate()->contains(reader.getRow().toString()))
            {
                continue;
            }

            if(pred.getColumnPredicate() &&
               !pred.getColumnPredicate()->contains(reader.getColumn().toString()))
            {
                continue;
            }

            if(pred.getTimePredicate() &&
               !pred.getTimePredicate()->contains(reader.getTimestamp()))
            {
                continue;
            }

            return true;
        }

        return false;
    }

public:
    BlockReader(strref_t data, ScanPredicate const & pred) :
        reader(data),
        pred(pred),
        hitStop(false)
    {
    }

    bool advance(CellKey & nextKey)
    {
        if(!hitStop && !moveToNext())
            return false;

        hitStop = false;
        nextKey = reader.getKey();
        return true;
    }
    
    void copyUntil(CellKey const * stopKey, bool filterErasures,
                   CellBuilder & out)
    {
        for(;;)
        {
            if(stopKey && !(reader.getKey() < *stopKey))
            {
                hitStop = true;
                break;
            }
            
            if(filterErasures && reader.isErasure())
            {
                if(!moveToNext())
                    break;
                continue;
            }
        }
    }
};


//----------------------------------------------------------------------------
// CellBuffer::Block
//----------------------------------------------------------------------------
class CellBuffer::Block
    : public FragmentBlock
{
    CellBuffer const & cells;

public:
    explicit Block(CellBuffer const & cells) : cells(cells) {}

    std::auto_ptr<FragmentBlockReader>
    makeReader(ScanPredicate const & pred) const
    {
        std::auto_ptr<FragmentBlockReader> p(
            new BlockReader(cells.data, pred));
        return p;
    }
};


//----------------------------------------------------------------------------
// CellBuffer
//----------------------------------------------------------------------------
CellBuffer::CellBuffer(strref_t data_)
{
    PackedCellReader reader(data_);
    
    if(!reader.verifyMagic())
        throw BadMagicError();
    
    if(!reader.verifyChecksum())
        throw BadChecksumError();
    
    if(!reader.verifyOffsets())
        throw CellDataError();

    nCells = 0;
    if(reader.next())
    {
        ++nCells;
        CellKeyRef last = reader.getKey();
        while(reader.next())
        {
            ++nCells;
            CellKeyRef key = reader.getKey();
            if(!(last < key))
                throw BadOrderError();
            last = key;
        }
    }

    data.insert(data.end(), data_.begin(), data_.end());
}

void CellBuffer::getRows(std::vector<warp::StringRange> & rows) const
{
    PackedCellReader reader(data);
    
    rows.clear();
    while(reader.next())
    {
        if(rows.empty() || rows.back() != reader.getRow())
            rows.push_back(reader.getRow());
    }
}

size_t CellBuffer::nextBlock(ScanPredicate const & pred, size_t minBlock) const
{
    if(minBlock == 0)
        return 0;
    else
        return size_t(-1);
}

std::auto_ptr<FragmentBlock> CellBuffer::loadBlock(size_t blockAddr) const
{
    assert(minBlock == 0);
    std::auto_ptr<FragmentBlock> p(new Block(*this));
    return p;
}
