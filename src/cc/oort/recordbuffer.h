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

#ifndef OORT_RECORDBUFFER_H
#define OORT_RECORDBUFFER_H

#include <oort/record.h>
#include <boost/utility.hpp>

namespace oort
{
    class RecordBuffer;
    class RecordBufferAllocator;
}


//----------------------------------------------------------------------------
// RecordBuffer
//----------------------------------------------------------------------------
class oort::RecordBuffer : public oort::RecordStore, public boost::noncopyable
{
public:
    enum { DATA_ALIGNMENT = sizeof(uintptr_t) };

    struct Header
    {
        enum { ALIGNMENT = 4 };
        uint32_t length;
        uint32_t typecode;
        uint16_t version;
        uint16_t flags;

        Header() {}
        Header(HeaderSpec::Fields const & f) :
            length(f.length),
            typecode(f.type),
            version(f.version),
            flags(f.flags) {}
    };

private:
    char * buf;
    char * end;

protected:
    virtual ~RecordBuffer();

public:
    explicit RecordBuffer(size_t bufSize);

    char * basePtr() const { return buf; }

    virtual uint32_t getLength(char const * ptr) const;
    virtual uint32_t getType(char const * ptr) const;
    virtual uint32_t getFlags(char const * ptr) const;
    virtual uint32_t getVersion(char const * ptr) const;
    virtual char const * getData(char const * ptr) const;
};


//----------------------------------------------------------------------------
// RecordBufferAllocator
//----------------------------------------------------------------------------
class oort::RecordBufferAllocator : public oort::Allocator,
                                    public boost::noncopyable
{
    enum { DEFAULT_ALLOC_SIZE = 16 << 10 };

    RecordBuffer * buf;
    char * bufPtr;
    char * bufEnd;
    size_t allocSize;
    size_t releasedSize;

    void allocBuf(size_t minSize);

public:
    explicit RecordBufferAllocator(size_t allocSize=DEFAULT_ALLOC_SIZE);
    virtual ~RecordBufferAllocator();

    virtual char * alloc(Record & r, HeaderSpec::Fields const & f);

    size_t getCommitSize() const;
    size_t getAllocSize() const;
    void resetReleased();
};


#endif // OORT_RECORDBUFFER_H
