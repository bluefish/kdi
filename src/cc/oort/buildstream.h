//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-03-08
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

#ifndef OORT_BUILDSTREAM_H
#define OORT_BUILDSTREAM_H

#include <oort/recordstream.h>
#include <oort/recordbuilder.h>
#include <oort/recordbuffer.h>
#include <boost/shared_ptr.hpp>

namespace oort
{
    class BuildStream;

    template <class T>
    class TypedBuildStream;

    template <class T>
    class FlushingTypedBuildStream;
}


//----------------------------------------------------------------------------
// BuildStream
//----------------------------------------------------------------------------
class oort::BuildStream : public RecordStream
{
public:
    typedef BuildStream my_t;
    typedef boost::shared_ptr<my_t> handle_t;

    typedef boost::shared_ptr<Allocator> alloc_t;

private:
    base_handle_t output;

protected:
    alloc_t alloc;

public:
    BuildStream(alloc_t const & alloc) :
        alloc(alloc)
    {
        assert(alloc);
    }

    void pipeTo(base_handle_t const & output)
    {
        this->output = output;
    }

    void put(Record const & r)
    {
        assert(output);
        output->put(r);
    }

    void flush()
    {
        assert(output);
        output->flush();
    }

    virtual void put(RecordBuilder & b) = 0;
};


//----------------------------------------------------------------------------
// TypedBuildStream
//----------------------------------------------------------------------------
template <class T>
class oort::TypedBuildStream : public BuildStream
{
public:
    typedef TypedBuildStream<T> my_t;
    typedef boost::shared_ptr<my_t> handle_t;
    typedef BuildStream super;
    
public:
    TypedBuildStream(alloc_t const & alloc) :
        super(alloc)
    {
    }

    using super::put;
    void put(RecordBuilder & b)
    {
        Record r;
        b.setHeader<T>();
        b.build(r, alloc.get());
        put(r);
    }
};


//----------------------------------------------------------------------------
// FlushingTypedBuildStream
//----------------------------------------------------------------------------
template <class T>
class oort::FlushingTypedBuildStream : public TypedBuildStream<T>
{
public:
    typedef FlushingTypedBuildStream<T> my_t;
    typedef boost::shared_ptr<my_t> handle_t;
    typedef TypedBuildStream<T> super;
    typedef typename super::alloc_t alloc_t;

private:
    size_t flushThreshold;
    size_t bytesAccumulated;
    
public:
    FlushingTypedBuildStream(alloc_t const & alloc, size_t flushThreshold) :
        super(alloc), flushThreshold(flushThreshold), bytesAccumulated(0)
    {
    }

    using super::put;
    void put(Record const & r)
    {
        super::put(r);
        bytesAccumulated += r.getLength();
        if(bytesAccumulated > flushThreshold)
            flush();
    }

    void flush()
    {
        super::flush();
        bytesAccumulated = 0;
    }
};


//----------------------------------------------------------------------------
// Builder builders
//----------------------------------------------------------------------------
namespace oort
{
    typedef BuildStream::handle_t BuildStreamHandle;

    template <class T>
    typename TypedBuildStream<T>::handle_t
    makeBuildStream(size_t allocSize)
    {
        BuildStream::alloc_t alloc(new RecordBufferAllocator(allocSize));
        typename TypedBuildStream<T>::handle_t
            bld(new TypedBuildStream<T>(alloc));
        return bld;
    }

    template <class T>
    typename TypedBuildStream<T>::handle_t
    makeBuildStream(BuildStream::alloc_t const & alloc)
    {
        typename TypedBuildStream<T>::handle_t
            bld(new TypedBuildStream<T>(alloc));
        return bld;
    }

    template <class T>
    typename FlushingTypedBuildStream<T>::handle_t
    makeBuildStream(size_t allocSize, size_t flushSize)
    {
        BuildStream::alloc_t alloc(new RecordBufferAllocator(allocSize));
        typename FlushingTypedBuildStream<T>::handle_t
            bld(new FlushingTypedBuildStream<T>(alloc, flushSize));
        return bld;
    }

    template <class T>
    typename FlushingTypedBuildStream<T>::handle_t
    makeBuildStream(BuildStream::alloc_t const & alloc, size_t flushSize)
    {
        typename FlushingTypedBuildStream<T>::handle_t
            bld(new FlushingTypedBuildStream<T>(alloc, flushSize));
        return bld;
    }
}


#endif // OORT_BUILDSTREAM_H
