//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-08
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

#include <kdi/local/disk_table_writer.h>
#include <kdi/local/table_types.h>

#include <warp/file.h>
#include <warp/log.h>
#include <ex/exception.h>
#include <oort/record.h>
#include <oort/recordstream.h>
#include <oort/recordbuilder.h>
#include <oort/recordbuffer.h>
#include <oort/fileio.h>
#include <warp/string_pool_builder.h>
#include <warp/adler.h>
#include <warp/bloom_filter.h>
#include <boost/static_assert.hpp>

#include <set>
#include <map>

using namespace kdi;
using namespace kdi::local;
using namespace oort;
using namespace warp;
using namespace ex;
using namespace std;

//----------------------------------------------------------------------------
// PooledBuilder
//----------------------------------------------------------------------------
namespace
{
    struct PooledBuilder
    {
        RecordBuilder builder;
        StringPoolBuilder pool;
        BuilderBlock * arr;
        BuilderBlock * fams;
        uint32_t nItems;
        bool addFams;
        uint32_t nFams;

        PooledBuilder() :
            builder(),
            pool(&builder),
            arr(builder.subblock(8)),
            nItems(0),
            addFams(false)
        {
        }

        void reset()
        {
            builder.reset();
            pool.reset(&builder);
            arr = builder.subblock(8);
            fams = builder.subblock(8);
            nItems = 0;
        }

        void build(Record & r, Allocator * alloc) {
            // Finish array
            builder.appendOffset(arr);
            builder.append(nItems);

            if(addFams) {
                builder.appendOffset(fams);
                builder.append(nFams);
            }

            // Construct record
            builder.build(r, alloc);
        }

        void write(RecordStreamHandle const & output, Record const & r)
        {
            output->put(r);
            reset();
        }

        void write(RecordStreamHandle const & output, Allocator * alloc)
        {
            Record r;
            build(r, alloc);
            write(output, r);
        }

        size_t getDataSize() const
        {
            return pool.getDataSize() + arr->size() + 8;
        }
    };

}

//----------------------------------------------------------------------------
// DiskTableWriter::Impl
//----------------------------------------------------------------------------
class DiskTableWriterV1::ImplV1 : public DiskTableWriter::Impl
{
    FilePtr fp;
    FileOutput::handle_t output;
    RecordBufferAllocator alloc;
    size_t blockSize;

    PooledBuilder block;
    PooledBuilder index;

    int64_t lowestTime;
    int64_t highestTime;

    // Maps string offsets in the index header to col family bitmasks
    map<size_t, uint32_t> colFamilyMasks;
    vector<size_t> allColFamilies;
    uint32_t nextColMask; // Mask to assign to the next column family
    uint32_t curColMask;  // Computed mask for the current cell block

    void addIndexEntry(Record const & cellBlock);
    void addCell(Cell const & x);
    void writeCellBlock();
    void writeBlockIndex();

public:
    explicit ImplV1(size_t blockSize);

    virtual void open(string const & fn);
    virtual void close();

    virtual void put(Cell const & x);

    virtual size_t size() const;
};

//----------------------------------------------------------------------------
// DiskTableWriterV1::ImplV1
//----------------------------------------------------------------------------
void DiskTableWriterV1::ImplV1::addIndexEntry(Record const & cbRec)
{
    BOOST_STATIC_ASSERT(disk::BlockIndexV1::VERSION == 1);

    // Get the last cell record from the cell block 
    disk::CellBlock const * cellBlock = cbRec.cast<disk::CellBlock>();
    disk::CellKey const & lastCellKey = cellBlock->cells[cellBlock->cells.size()-1].key;
    strref_t lastRow = *lastCellKey.row;

    // Get string offset for cell key (just the last row)
    BuilderBlock * b = index.pool.getStringBlock();
    size_t         r = index.pool.getStringOffset(lastRow);

    // Calculate Adler-32 checksum for the cell block, written in index 
    uint32_t cbChecksum = adler32((uint8_t*)cbRec.getData(), cbRec.getLength());

    // Append IndexEntry to array
    index.arr->append(cbChecksum);   // checkSum
    index.arr->appendOffset(b, r);   // row
    index.arr->append(fp->tell());   // blockOffset
    index.arr->append(lowestTime);   // timeRange-min
    index.arr->append(highestTime);  // timeRange-max
    index.arr->append(curColMask);   // column family mask
    curColMask = 0;

    // Pad record out to full alignment
    index.arr->appendPadding(8);

    ++index.nItems;
}

void DiskTableWriterV1::ImplV1::addCell(Cell const & x)
{
    BOOST_STATIC_ASSERT(disk::CellBlock::VERSION == 0);

    // Get string offsets for cell data (null value for erasures)
    BuilderBlock * b = block.pool.getStringBlock();
    size_t         r = block.pool.getStringOffset(x.getRow());
    size_t         c = block.pool.getStringOffset(x.getColumn());
    int64_t        t = x.getTimestamp();

    // Append CellData to array
    block.arr->appendOffset(b, r);         // key.row
    block.arr->appendOffset(b, c);         // key.column
    block.arr->append(t);                  // key.timestamp
    if(!x.isErasure())
    {
        size_t v = block.pool.getStringOffset(x.getValue());
        block.arr->appendOffset(b, v);     // value
    }
    else
    {
        block.arr->appendOffset(0);        // value
    }
    block.arr->append<uint32_t>(0);        // __pad
    ++block.nItems;

    // Remember range of timestamps added
    if(block.nItems == 1) {
        lowestTime = t;
        highestTime = t;
    } else {
        if(t < lowestTime) lowestTime = t;
        if(t > highestTime) highestTime = t;
    }

    // Update the column family lookup and mask
    size_t colFamily = index.pool.getStringOffset(x.getColumnFamily());
    if(colFamilyMasks.count(colFamily) == 1) {
        curColMask |= colFamilyMasks[colFamily];
    } else {
        // The mask is allowed to wrap around!
        // If there are more than 32 column families (rare use case),
        // it simply results in false positives
        if(nextColMask == 0) nextColMask = 1;

        colFamilyMasks[colFamily] = nextColMask;
        curColMask |= nextColMask;
        nextColMask = nextColMask << 1;
    }
}

void DiskTableWriterV1::ImplV1::writeCellBlock()
{
    Record r;
    block.build(r, &alloc);

    /* Create the index entry */
    addIndexEntry(r);

    /* Write out the block */
    block.write(output, r);
}

void DiskTableWriterV1::ImplV1::writeBlockIndex()
{
    BuilderBlock * b = index.pool.getStringBlock();
    uint32_t nFams = 0;
    
    map<size_t, uint32_t>::const_iterator mi;
    for(mi = colFamilyMasks.begin(); mi != colFamilyMasks.end(); ++mi) {
        pair<size_t, uint32_t> const & p = *mi;
        index.fams->appendOffset(b, p.first);
        ++nFams;
    }

    index.addFams = true;
    index.nFams = nFams;
    index.write(output, &alloc);
}

DiskTableWriterV1::ImplV1::ImplV1(size_t blockSize) :
    alloc(),
    blockSize(blockSize)
{
    block.builder.setHeader<disk::CellBlock>();
    index.builder.setHeader<disk::BlockIndexV1>();
}

void DiskTableWriterV1::ImplV1::open(string const & fn)
{
    fp = File::output(fn);
    output = FileOutput::make(fp);

    block.reset();
    index.reset();

    lowestTime = 0;
    highestTime = 0;

    colFamilyMasks.clear();
    nextColMask = 1;
    curColMask = 0;
}

void DiskTableWriterV1::ImplV1::close()
{
    // Flush last cell block if there's something pending
    if(block.nItems) {
        writeCellBlock();
    }

    // Remember index position
    uint64_t indexOffset = fp->tell();

    // Write BlockIndex record
    writeBlockIndex();

    // Write TableInfo record
    Record r;
    alloc.construct<disk::TableInfo>(r, indexOffset);
    output->put(r);

    // Shut down
    output->flush();
    fp->close();
    output.reset();
    fp.reset();
}

void DiskTableWriterV1::ImplV1::put(Cell const & x)
{
    // Add cell to the current block
    addCell(x);

    // Flush block if it is big enough
    if(block.getDataSize() >= blockSize) {
        writeCellBlock();
    }
}

size_t DiskTableWriterV1::ImplV1::size() const
{
    return fp->tell() + block.getDataSize() + index.getDataSize();
}

DiskTableWriterV1::DiskTableWriterV1(size_t blockSize) :
    DiskTableWriter(new ImplV1(blockSize))
{
}

//----------------------------------------------------------------------------
// DiskTableWriter
//----------------------------------------------------------------------------
DiskTableWriter::~DiskTableWriter()
{
    if(!closed)
        close();
}

void DiskTableWriter::open(std::string const & fn)
{
    if(!closed)
        raise<RuntimeError>("DiskTableWriter already open");

    impl->open(fn);
    closed = false;
}

void DiskTableWriter::close()
{
    if(closed)
        raise<RuntimeError>("DiskTableWriter already closed");

    impl->close();
    closed = true;
}

void DiskTableWriter::put(Cell const & x)
{
    if(closed)
        raise<RuntimeError>("put() to closed DiskTableWriter");

    impl->put(x);
}

size_t DiskTableWriter::size() const
{
    if(closed)
        raise<RuntimeError>("size() to closed DiskTableWriter");

    return impl->size();
}
