//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-11
// 
// This file is part of the oort library.
// 
// The oort library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The oort library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#include <oort/recordbuffer.h>
#include <warp/util.h>

using namespace oort;
using namespace warp;

//#define OORT_RBSPAM 1

#ifdef OORT_RBSPAM
#include <iostream>
#include <boost/format.hpp>
#include <warp/strutil.h>
using namespace std;

inline void reportDelta(bool isAlloc, size_t nBytes)
{
    using boost::format;

    static size_t nBufs = 0;
    static size_t total = 0;

    if(isAlloc)
    {
        ++nBufs;
        total += nBytes;
        cerr << format("alloc   %s  (%s %s)\n") %
            sizeString(nBytes) % nBufs % sizeString(total);
    }
    else
    {
        --nBufs;
        total -= nBytes;
        cerr << format("dealloc %s  (%s %s)\n") %
            sizeString(nBytes) % nBufs % sizeString(total);
    }
}

#endif


//----------------------------------------------------------------------------
// header_cast
//----------------------------------------------------------------------------
namespace
{
    inline RecordBuffer::Header const * header_cast(void const * ptr)
    {
        return reinterpret_cast<RecordBuffer::Header const *>(ptr);
    }
}


//----------------------------------------------------------------------------
// RecordBuffer
//----------------------------------------------------------------------------
RecordBuffer::~RecordBuffer()
{
#ifdef OORT_RBSPAM
    reportDelta(false, end - buf);
#endif
    delete[] buf;
}

RecordBuffer::RecordBuffer(size_t bufSize) :
    buf(bufSize ? new char[bufSize] : 0), end(buf + bufSize)
{
#ifdef OORT_RBSPAM
    reportDelta(true, end - buf);
#endif
}

uint32_t RecordBuffer::getLength(char const * ptr) const
{
    assert(ptr >= buf && ptr + sizeof(Header) <= end);
    return header_cast(ptr)->length;
}

uint32_t RecordBuffer::getType(char const * ptr) const
{
    assert(ptr >= buf && ptr + sizeof(Header) <= end);
    return header_cast(ptr)->typecode;
}

uint32_t RecordBuffer::getFlags(char const * ptr) const
{
    assert(ptr >= buf && ptr + sizeof(Header) <= end);
    return header_cast(ptr)->flags;
}

uint32_t RecordBuffer::getVersion(char const * ptr) const
{
    assert(ptr >= buf && ptr + sizeof(Header) <= end);
    return header_cast(ptr)->version;
}

char const * RecordBuffer::getData(char const * ptr) const
{
    char const * dPtr = alignUp(ptr + sizeof(Header), DATA_ALIGNMENT);
    assert(dPtr >= buf && dPtr + getLength(ptr) <= end);
    return dPtr;
}


//----------------------------------------------------------------------------
// RecordBufferAllocator
//----------------------------------------------------------------------------
void RecordBufferAllocator::allocBuf(size_t minSize)
{
    if(minSize < allocSize)
        minSize = allocSize;

    if(buf)
    {
        releasedSize += bufEnd - buf->basePtr();
        buf->release();
    }

    buf = new RecordBuffer(minSize);
    bufPtr = buf->basePtr();
    bufEnd = bufPtr + minSize;
}
        
RecordBufferAllocator::RecordBufferAllocator(size_t allocSize) :
    buf(0), bufPtr(0), bufEnd(0), allocSize(allocSize), releasedSize(0)
{
}

RecordBufferAllocator::~RecordBufferAllocator()
{
    if(buf)
        buf->release();
}

char * RecordBufferAllocator::alloc(Record & r, HeaderSpec::Fields const & f)
{
    // Reuse memory if nobody else has a reference to it
    r.release();
    if(buf && buf->unique())
        bufPtr = buf->basePtr();

    RecordBuffer::Header hdr(f);
    for(;;)
    {
        char * hdrPtr = alignUp(bufPtr, RecordBuffer::Header::ALIGNMENT);
        char * dataPtr = alignUp(hdrPtr + sizeof(hdr), RecordBuffer::DATA_ALIGNMENT);
        char * dataEnd = dataPtr + hdr.length;

        if(dataEnd <= bufEnd)
        {
#if defined(OORT_RBSPAM) && OORT_RBSPAM > 1
            void * bufBase = 0;
            if(buf)
                bufBase = buf->basePtr();
            off_t bufOff = hdrPtr - (char *)bufBase;
            cerr << "alloc : " << bufBase << '[' << bufOff << "] : "
                 << (dataEnd - hdrPtr) << endl;
#endif

            serialize(hdrPtr, hdr);
            r = Record(buf, hdrPtr);
            bufPtr = dataEnd;
            return dataPtr;
        }
        else
        {
            allocBuf(dataEnd - hdrPtr);
        }
    }
}

size_t RecordBufferAllocator::getCommitSize() const
{
    if(buf && !buf->unique())
        return releasedSize + (bufPtr - buf->basePtr());
    else
        return releasedSize;
}

size_t RecordBufferAllocator::getAllocSize() const
{
    if(buf)
        return releasedSize + (bufEnd - buf->basePtr());
    else
        return releasedSize;
}

void RecordBufferAllocator::resetReleased()
{
    releasedSize = 0;
}
