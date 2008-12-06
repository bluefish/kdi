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
// DiskTableWriter::Impl
//----------------------------------------------------------------------------
class DiskTableWriter::Impl
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
    explicit Impl(size_t blockSize);

    void open(string const & fn);
    void close();

    void put(Cell const & x);

    size_t size() const;
};

//----------------------------------------------------------------------------
// DiskTableWriter::Impl
//----------------------------------------------------------------------------
void DiskTableWriter::Impl::addIndexEntry(Cell const & x)
{
    BOOST_STATIC_ASSERT(disk::BlockIndex::VERSION == 0);

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

void DiskTableWriter::Impl::addCell(Cell const & x)
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

void DiskTableWriter::Impl::writeCellBlock()
{
    block.write(output, &alloc);
}

void DiskTableWriter::Impl::writeBlockIndex()
{
    index.write(output, &alloc);
}

DiskTableWriter::Impl::Impl(size_t blockSize) :
    alloc(),
    blockSize(blockSize)
{
    block.builder.setHeader<disk::CellBlock>();
    index.builder.setHeader<disk::BlockIndex>();
}

void DiskTableWriter::Impl::open(string const & fn)
{
    fp = File::output(fn);
    output = FileOutput::make(fp);

    block.reset();
    index.reset();
}

void DiskTableWriter::Impl::close()
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
    alloc.construct<disk::TableInfo>(r, indexOffset);
    output->put(r);

    // Shut down
    output->flush();
    fp->close();
    output.reset();
    fp.reset();
}

void DiskTableWriter::Impl::put(Cell const & x)
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

size_t DiskTableWriter::Impl::size() const
{
    return fp->tell() + block.getDataSize() + index.getDataSize();
}


//----------------------------------------------------------------------------
// DiskTableWriter
//----------------------------------------------------------------------------
DiskTableWriter::DiskTableWriter(size_t blockSize) :
    impl(new Impl(blockSize)), closed(true)
{
}

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
