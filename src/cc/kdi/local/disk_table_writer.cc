//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/local/disk_table_writer.cc#1 $
//
// Created 2007/10/08
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
            pool.reset();
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
    Impl(string const & fn, size_t blockSize);
    void close();
    void put(Cell const & x);
};

//----------------------------------------------------------------------------
// DiskTableWriter::Impl
//----------------------------------------------------------------------------
void DiskTableWriter::Impl::addIndexEntry(Cell const & x)
{
    BOOST_STATIC_ASSERT(disk::BlockIndex::VERSION == 0);

    // Get string offsets for cell key
    BuilderBlock * r = index.pool.get(x.getRow());
    BuilderBlock * c = index.pool.get(x.getColumn());
    int64_t        t = x.getTimestamp();
    uint64_t       o = fp->tell();
            
    // Append IndexEntry to array
    index.arr->appendOffset(r);     // startKey.row
    index.arr->appendOffset(c);     // startKey.column
    index.arr->append(t);           // startKey.timestamp
    index.arr->append(o);           // blockOffset
    ++index.nItems;
}

void DiskTableWriter::Impl::addCell(Cell const & x)
{
    BOOST_STATIC_ASSERT(disk::CellBlock::VERSION == 0);

    // Get string offsets for cell data (null value for erasures)
    BuilderBlock * r = block.pool.get(x.getRow());
    BuilderBlock * c = block.pool.get(x.getColumn());
    int64_t        t = x.getTimestamp();
    BuilderBlock * v = x.isErasure() ? 0 : block.pool.get(x.getValue());

    // Append CellData to array
    block.arr->appendOffset(r);            // key.row
    block.arr->appendOffset(c);            // key.column
    block.arr->append(t);                  // key.timestamp
    block.arr->appendOffset(v);            // value
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

DiskTableWriter::Impl::Impl(string const & fn, size_t blockSize) :
    fp(File::output(fn)),
    output(FileOutput::make(fp)),
    alloc(),
    blockSize(blockSize)
{
    block.builder.setHeader<disk::CellBlock>();
    index.builder.setHeader<disk::BlockIndex>();
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


//----------------------------------------------------------------------------
// DiskTableWriter
//----------------------------------------------------------------------------
DiskTableWriter::DiskTableWriter(std::string const & fn, size_t blockSize) :
    impl(new Impl(fn, blockSize))
{
}

DiskTableWriter::~DiskTableWriter()
{
    close();
}

void DiskTableWriter::close()
{
    if(impl)
    {
        impl->close();
        impl.reset();
    }
}

void DiskTableWriter::put(Cell const & x)
{
    if(!impl)
        raise<RuntimeError>("put() to closed writer");
        
    impl->put(x);
}
