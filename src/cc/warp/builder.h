//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-12
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef WARP_BUILDER_H
#define WARP_BUILDER_H

#include "util.h"
#include "offset.h"
#include <vector>
#include <utility>
#include <string.h>
#include <assert.h>
#include <boost/utility.hpp>

namespace warp
{
    class BuilderBlock;
    class Builder;
}


//----------------------------------------------------------------------------
// BuilderBlock
//----------------------------------------------------------------------------
class warp::BuilderBlock : boost::noncopyable
{
    typedef std::pair<BuilderBlock *, size_t> fixup_t;
    typedef std::vector<fixup_t> fixup_vec_t;

    friend class Builder;
    Builder * builder;
    fixup_vec_t backRefs;

    char * buf;
    size_t bufSz;
    size_t bufCap;

    size_t align;
    size_t baseAddr;
    size_t alignPadding;
    bool hasBaseAddr;

private:
    inline BuilderBlock(Builder * builder);
    inline ~BuilderBlock();

    inline void init(size_t align);
    inline void addRef(BuilderBlock * block, size_t offset);

    inline size_t assignBase(size_t base);
    inline void unassignBase();

    inline void fixupReferences();
    inline void setReference(size_t refPos, size_t targetAddr);

    inline void dumpBlock(char * basePtr);

public:
    /// Create a floating subblock within the resource that can be
    /// referenced from other blocks via an offset.  The alignment
    /// parameter determines the memory alignment of the beginning of
    /// the block.
    inline BuilderBlock * subblock(size_t align);

    /// Return true if the resource has been finalized.
    inline bool isFinalized() const;

    /// Write some data at the given position.  The block will expand
    /// if necessary to accomodate the data.
    inline void write(size_t pos, void const * ptr, size_t len);

    /// Convenience method to write types that can be directly copied.
    template <class T> inline void write(size_t pos, T const & val);

    /// Write an offset to another block at the given position.  The
    /// target block may be null.
    inline void writeOffset(size_t pos, BuilderBlock * target);

    /// Append some data to the end of the block.
    inline void append(void const * ptr, size_t len);

    /// Convenience method to append types that can be directly copied.
    template <class T> inline void append(T const & val);

    /// Append an offset to another block at the end of the block.  The
    /// target block may be null.
    inline void appendOffset(BuilderBlock * target);

    /// Get the current size of the data in this block.
    inline size_t size() const;

    /// Get the start pointer of the data contained in the block.
    inline char const * begin() const;
    
    /// Get the end pointer of the data contained in the block.
    inline char const * end() const;
};


//----------------------------------------------------------------------------
// Builder
//----------------------------------------------------------------------------
class warp::Builder : public warp::BuilderBlock
{
    typedef std::vector<BuilderBlock *> block_vec_t;

    block_vec_t subblocks;
    size_t endBlock;
    size_t finalSize;
    bool hasFinalSize;

public:
    inline Builder();
    inline ~Builder();

    /// Create a floating subblock within the resource that can be
    /// referenced from other blocks via an offset.  The alignment
    /// parameter determines the memory alignment of the beginning of
    /// the block.
    inline BuilderBlock * subblock(size_t align);

    /// Finalize all offset references and assign addresses to
    /// subblocks.  The data is ready to export after this call.
    inline void finalize();

    /// Get total size of finalized data.
    inline size_t getFinalSize() const;

    /// Return true if the resource has been finalized.
    inline bool isFinalized() const;

    /// Export finalized data to the given address.  The memory size
    /// necessary can be obtained via getFinalSize().
    inline void exportTo(void * dst);

    /// Unfinalize data and allow continued writing.
    inline void resume();

    /// Clear data in builder and prepare to start a new resource.
    inline void reset();

    /// Convenience method to finalize, export, and reset.  The memory
    /// for the exported resource is allocated with new and returned
    /// from this function.  The size of the allocated memory is
    /// returned via the optional rSize parameter, if non-null.
    inline void * build(size_t * rSize=0);
};


//----------------------------------------------------------------------------
// BuilderBlock implementation
//----------------------------------------------------------------------------
namespace warp
{
    BuilderBlock::BuilderBlock(Builder * builder) :
        builder(builder),
        buf(0), bufSz(0), bufCap(0),
        align(1),
        hasBaseAddr(false)
    {
    }

    BuilderBlock::~BuilderBlock()
    {
        delete[] buf;
    }

    void BuilderBlock::init(size_t align)
    {
        assert(isPowerOf2(align));
        this->align = align;
    
        hasBaseAddr = false;
        bufSz = 0;
        backRefs.clear();
    }
            
    void BuilderBlock::addRef(BuilderBlock * block, size_t offset)
    {
        backRefs.push_back(fixup_t(block, offset));
    }

    size_t BuilderBlock::assignBase(size_t base)
    {
        if(!hasBaseAddr)
        {
            baseAddr = alignUp(base, align);
            alignPadding = baseAddr - base;
            hasBaseAddr = true;
            return baseAddr + bufSz;
        }
        else
        {
            assert(base >= baseAddr + bufSz);
            return base;
        }
    }

    void BuilderBlock::unassignBase()
    {
        assert(hasBaseAddr);
        hasBaseAddr = false;
    }

    void BuilderBlock::fixupReferences()
    {
        assert(hasBaseAddr);
        for(fixup_vec_t::const_iterator ri = backRefs.begin();
            ri != backRefs.end(); ++ri)
        {
            ri->first->setReference(ri->second, baseAddr);
        }
    }

    void BuilderBlock::setReference(size_t refPos, size_t targetAddr)
    {
        assert(hasBaseAddr);
        off_t offsetValue = targetAddr;
        offsetValue -= refPos + baseAddr;
        assert(inNumericRange<int32_t>(offsetValue));
        write<int32_t>(refPos, offsetValue);
    }

    void BuilderBlock::dumpBlock(char * basePtr)
    {
        assert(hasBaseAddr);
        if(alignPadding)
            memset(&basePtr[baseAddr-alignPadding], 0, alignPadding);
        memcpy(&basePtr[baseAddr], buf, bufSz);
    }

    BuilderBlock * BuilderBlock::subblock(size_t align)
    {
        // Forward to builder's implementation
        assert(builder);
        return builder->subblock(align);
    }

    bool BuilderBlock::isFinalized() const
    {
        // Forward to builder's implementation
        assert(builder);
        return builder->isFinalized();
    }

    void BuilderBlock::write(size_t pos, void const * ptr, size_t len)
    {
        assert(!isFinalized());

        size_t end = pos + len;
        if(end > bufSz)
        {
            if(end > bufCap)
            {
                bufCap = std::max(end, bufCap << 1);
                char * newBuf = new char[bufCap];
                memcpy(newBuf, buf, bufSz);
                std::swap(buf, newBuf);
                delete[] newBuf;
            }
            bufSz = end;
        }
        memcpy(&buf[pos], ptr, len);
    }

    template <class T>
    void BuilderBlock::write(size_t pos, T const & val)
    {
        write(pos, &val, sizeof(T));
    }

    void BuilderBlock::writeOffset(size_t pos, BuilderBlock * target)
    {
        // Must have same builder (or be a null reference)
        assert(!target || builder == target->builder);

        if(target == this)
        {
            // Self-references can be resolved immediately
            write<int32_t>(pos, -int32_t(pos));
        }
        else
        {
            // Interblock references must be deferred until
            // finalization.
            if(target)
                target->addRef(this, pos);

            // Write null reference.
            write<int32_t>(pos, OffsetBase<int32_t>::NULL_OFFSET);
        }
    }

    void BuilderBlock::append(void const * ptr, size_t len)
    {
        write(bufSz, ptr, len);
    }

    template <class T>
    void BuilderBlock::append(T const & val)
    {
        write(bufSz, &val, sizeof(T));
    }

    void BuilderBlock::appendOffset(BuilderBlock * target)
    {
        writeOffset(bufSz, target);
    }

    size_t BuilderBlock::size() const
    {
        return bufSz;
    }

    char const * BuilderBlock::begin() const
    {
        return buf;
    }
    
    char const * BuilderBlock::end() const
    {
        return buf + bufSz;
    }
}


//----------------------------------------------------------------------------
// Builder implementation
//----------------------------------------------------------------------------
namespace warp
{
    Builder::Builder() :
        BuilderBlock(this), endBlock(0), hasFinalSize(false)
    {
    }

    Builder::~Builder()
    {
        for(block_vec_t::iterator bi = subblocks.begin();
            bi != subblocks.end(); ++bi)
        {
            delete *bi;
        }
    }

    BuilderBlock * Builder::subblock(size_t align)
    {
        BuilderBlock * b;
        if(endBlock < subblocks.size())
            b = subblocks[endBlock];
        else
        {
            b = new BuilderBlock(this);
            subblocks.push_back(b);
        }
        ++endBlock;
        b->init(align);
        return b;
    }

    void Builder::finalize()
    {
        assert(!hasFinalSize);

        // Set base address for each block.
        size_t topAddr = this->assignBase(0);
        for(block_vec_t::iterator bi = subblocks.begin(), bEnd = bi+endBlock;
            bi != bEnd; ++bi)
        {
            topAddr = (*bi)->assignBase(topAddr);
        }
        
        // Block addresses have been assigned, fixup offset references
        fixupReferences();
        for(block_vec_t::iterator bi = subblocks.begin(), bEnd = bi+endBlock;
            bi != bEnd; ++bi)
        {
            (*bi)->fixupReferences();
        }

        // topAddr is now equal to full record length
        finalSize = topAddr;
        hasFinalSize = true;
    }

    size_t Builder::getFinalSize() const
    {
        assert(hasFinalSize);
        return finalSize;
    }

    bool Builder::isFinalized() const
    {
        return hasFinalSize;
    }

    void Builder::exportTo(void * dst)
    {
        assert(hasFinalSize);

        // Copy blocks
        char * base = reinterpret_cast<char *>(dst);
        this->dumpBlock(base);
        for(block_vec_t::iterator bi = subblocks.begin(), bEnd = bi+endBlock;
            bi != bEnd; ++bi)
        {
            (*bi)->dumpBlock(base);
        }
    }

    void Builder::resume()
    {
        assert(hasFinalSize);
        this->unassignBase();
        for(block_vec_t::iterator bi = subblocks.begin(), bEnd = bi+endBlock;
            bi != bEnd; ++bi)
        {
            (*bi)->unassignBase();
        }
        hasFinalSize = false;
    }

    void Builder::reset()
    {
        // Done and done
        init(1);
        endBlock = 0;
        hasFinalSize = false;
    }

    void * Builder::build(size_t * rSize)
    {
        // Finalize, allocate data, export, and reset
        finalize();
        char * dataPtr = new char[finalSize];
        if(rSize)
            *rSize = finalSize;
        exportTo(dataPtr);
        reset();
        return dataPtr;
    }
}


//----------------------------------------------------------------------------
// C++ style serialization
//----------------------------------------------------------------------------
namespace warp
{
    // Primitive types
    inline BuilderBlock & operator<<(BuilderBlock & b, uint8_t x) {
        b.append(x);
        return b;
    }
    inline BuilderBlock & operator<<(BuilderBlock & b, uint16_t x) {
        b.append(x);
        return b;
    }
    inline BuilderBlock & operator<<(BuilderBlock & b, uint32_t x) {
        b.append(x);
        return b;
    }
    inline BuilderBlock & operator<<(BuilderBlock & b, uint64_t x) {
        b.append(x);
        return b;
    }
    inline BuilderBlock & operator<<(BuilderBlock & b, int8_t x) {
        b.append(x);
        return b;
    }
    inline BuilderBlock & operator<<(BuilderBlock & b, int16_t x) {
        b.append(x);
        return b;
    }
    inline BuilderBlock & operator<<(BuilderBlock & b, int32_t x) {
        b.append(x);
        return b;
    }
    inline BuilderBlock & operator<<(BuilderBlock & b, int64_t x) {
        b.append(x);
        return b;
    }
    inline BuilderBlock & operator<<(BuilderBlock & b, float x) {
        b.append(x);
        return b;
    }
    inline BuilderBlock & operator<<(BuilderBlock & b, double x) {
        b.append(x);
        return b;
    }

    // Offsets
    inline BuilderBlock & operator<<(BuilderBlock & b, BuilderBlock & sub) {
        b.appendOffset(&sub);
        return b;
    }
}

#endif // WARP_BUILDER_H

