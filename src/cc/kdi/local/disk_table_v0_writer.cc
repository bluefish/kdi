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
#include <ex/exception.h>
#include <oort/record.h>
#include <oort/recordstream.h>
#include <oort/recordbuilder.h>
#include <oort/recordbuffer.h>
#include <oort/fileio.h>
#include <warp/string_pool_builder.h>
#include <boost/static_assert.hpp>

using namespace kdi;
using namespace kdi::local;
using namespace oort;
using namespace warp;
using namespace ex;
using namespace std;

/*
 * DEPRECATED
 *
 * This file contains the writer for the original disk table implementation.
 * It is preservered only for purposes of testing the old disk table reader.
 * 
 */

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
        uint32_t nItems;

        PooledBuilder() :
            builder(),
            pool(&builder),
            arr(builder.subblock(8)),
            nItems(0)
        {
        }

        void reset()
        {
            builder.reset();
            pool.reset(&builder);
            arr = builder.subblock(8);
            nItems = 0;
        }

        void write(RecordStreamHandle const & output, Allocator * alloc)
        {
            // Finish array
            builder.appendOffset(arr);
            builder.append(nItems);

            // Write CellBlock record
            Record r;
            builder.build(r, alloc);
            output->put(r);

            // Reset
            reset();
        }

        size_t getDataSize() const
        {
            return pool.getDataSize() + arr->size() + 8;
        }
    };
}

//----------------------------------------------------------------------------
// DiskTableWriterV0::ImplV0
//----------------------------------------------------------------------------
class DiskTableWriterV0::ImplV0 : public DiskTableWriter::Impl
{
    FilePtr fp;
    FileOutput::handle_t output;
    RecordBufferAllocator alloc;
    size_t blockSize;

    PooledBuilder block;
    PooledBuilder index;

    void addIndexEntry(Cell const & x);
    void addCell(Cell const & x);
    void writeCellBlock();
    void writeBlockIndex();

public:
    explicit ImplV0(size_t blockSize);

    void open(string const & fn);
    void close();

    void put(Cell const & x);

    size_t size() const;
};

//----------------------------------------------------------------------------
// DiskTableWriterV0::ImplV0
//----------------------------------------------------------------------------
void DiskTableWriterV0::ImplV0::addIndexEntry(Cell const & x)
{
    BOOST_STATIC_ASSERT(disk::BlockIndexV0::VERSION == 0);

    // Get string offsets for cell key
    BuilderBlock * b = index.pool.getStringBlock();
    size_t         r = index.pool.getStringOffset(x.getRow());
    size_t         c = index.pool.getStringOffset(x.getColumn());
    int64_t        t = x.getTimestamp();
    uint64_t       o = fp->tell();
            
    // Append IndexEntry to array
    index.arr->appendOffset(b, r);  // startKey.row
    index.arr->appendOffset(b, c);  // startKey.column
    index.arr->append(t);           // startKey.timestamp
    index.arr->append(o);           // blockOffset
    ++index.nItems;
}

void DiskTableWriterV0::ImplV0::addCell(Cell const & x)
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
}

void DiskTableWriterV0::ImplV0::writeCellBlock()
{
    block.write(output, &alloc);
}

void DiskTableWriterV0::ImplV0::writeBlockIndex()
{
    index.write(output, &alloc);
}

DiskTableWriterV0::ImplV0::ImplV0(size_t blockSize) :
    alloc(),
    blockSize(blockSize)
{
    block.builder.setHeader<disk::CellBlock>();
    index.builder.setHeader<disk::BlockIndexV0>();
}

void DiskTableWriterV0::ImplV0::open(string const & fn)
{
    fp = File::output(fn);
    output = FileOutput::make(fp);

    block.reset();
    index.reset();
}

void DiskTableWriterV0::ImplV0::close()
{
    // Flush last cell block if there's something pending
    if(block.nItems)
        writeCellBlock();

    // Remember index position
    uint64_t indexOffset = fp->tell();

    // Write BlockIndex record
    writeBlockIndex();

    // Write TableInfo record
    Record r;
    HeaderSpec::Fields f;
    f.setFromType<disk::TableInfo>();
    f.version = 0;              // This is an old version
    serialize<uint64_t>(alloc.alloc(r,f), indexOffset);
    output->put(r);

    // Shut down
    output->flush();
    fp->close();
    output.reset();
    fp.reset();
}

void DiskTableWriterV0::ImplV0::put(Cell const & x)
{
    // If this is the first Cell in the block, write index record
    if(!block.nItems)
        addIndexEntry(x);

    // Add cell to the current block
    addCell(x);

    // Flush block if it is big enough
    if(block.getDataSize() >= blockSize)
        writeCellBlock();
}

size_t DiskTableWriterV0::ImplV0::size() const
{
    return fp->tell() + block.getDataSize() + index.getDataSize();
}

//----------------------------------------------------------------------------
// DiskTableWriterV0
//----------------------------------------------------------------------------
DiskTableWriterV0::DiskTableWriterV0(size_t blockSize) :
    DiskTableWriter(new ImplV0(blockSize))
{
}
//
