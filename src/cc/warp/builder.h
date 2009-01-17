//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-12
// 
// This file is part of the warp library.
// 
// The warp library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The warp library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef WARP_BUILDER_H
#define WARP_BUILDER_H

#include <warp/util.h>
#include <warp/offset.h>
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
    struct BackRef
    {
        BuilderBlock * srcBlock;
        size_t srcPos;
        size_t dstPos;

        BackRef() {}
        BackRef(BuilderBlock * srcBlock, size_t srcPos, size_t dstPos) :
            srcBlock(srcBlock), srcPos(srcPos), dstPos(dstPos) {}
    };
    typedef std::vector<BackRef> backref_vec_t;

    friend class Builder;
    Builder * builder;
    backref_vec_t backRefs;

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

    /// Init/reset the block.  The data size goes back to zero and all
    /// back-refs are cleared.  No memory is released.
    inline void init(size_t align);

    /// Add the given (block,offset) pair to the back-ref list.  The
    /// address is the location of the reference to this block.  It
    /// will fixed up later after this block has been given its final
    /// address.
    inline void addBackReference(BuilderBlock * srcBlock, size_t srcPos,
                                 size_t dstPos);

    /// Set the final address for this block.  The given base is the
    /// earliest position this block can occupy.  If this block has
    /// alignment requirements, it will advance the base to the
    /// nearest aligned position and use that.  After the base is
    /// assigned, the block is "final".
    inline size_t assignBase(size_t base);

    /// Remove the "final" flag from the block so more modifications
    /// can be made.
    inline void unassignBase();

    /// Go through back-ref list and fixup references to this block to
    /// point to their final address.
    inline void fixupReferences();

    /// Update the reference at refPos point to the target block at
    /// targetAddr.  targetAddr should be the base of the destination
    /// block.  If the original reference had a target offset, it will
    /// be added within.
    inline void setReference(size_t refPos, size_t targetAddr);

    /// Copy the contents of the block to given destination.  If this
    /// block has some alignment padding, the padding area before the
    /// basePtr will be zeroed.
    inline void dumpBlock(char * basePtr);

    /// Grow the block to contain at least minSz bytes.  If growing
    /// makes an uninitialized hole between the old bufSz and clearTo,
    /// fill the hole with zeros.
    inline void grow(size_t minSz, size_t clearTo);

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
    /// target block may be null.  If targetOffset is greater than
    /// zero, the offset will refer to a point within the target
    /// block, instead of the beginning.
    inline void writeOffset(size_t pos, BuilderBlock * target,
                            size_t targetOffset=0);

    /// Append some data to the end of the block.
    inline void append(void const * ptr, size_t len);

    /// Convenience method to append types that can be directly copied.
    template <class T> inline void append(T const & val);

    /// Append an offset to another block at the end of the block.
    /// The target block may be null.  If targetOffset is greater than
    /// zero, the offset will refer to a point within the target
    /// block, instead of the beginning.
    inline void appendOffset(BuilderBlock * target, size_t targetOffset=0);

    /// Append padding until the size of the block is an even multiple
    /// of the given alignment.
    inline void appendPadding(size_t toAlignment);

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
            
    void BuilderBlock::addBackReference(BuilderBlock * srcBlock, size_t srcPos,
                                        size_t dstPos)
    {
        backRefs.push_back(BackRef(srcBlock, srcPos, dstPos));
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
        for(backref_vec_t::const_iterator ri = backRefs.begin();
            ri != backRefs.end(); ++ri)
        {
            assert(ri->dstPos <= bufSz);
            ri->srcBlock->setReference(ri->srcPos, baseAddr + ri->dstPos);
        }
    }

    void BuilderBlock::setReference(size_t refPos, size_t targetAddr)
    {
        assert(hasBaseAddr);

        // Compute offset to target
        off_t offsetValue = targetAddr;
        offsetValue -= refPos + baseAddr;

        // Write offset with final value
        assert(inNumericRange<int32_t>(offsetValue));
        assert(int32_t(offsetValue) != OffsetBase<int32_t>::NULL_OFFSET);
        write<int32_t>(refPos, offsetValue);
    }

    void BuilderBlock::dumpBlock(char * basePtr)
    {
        assert(hasBaseAddr);
        if(alignPadding)
            memset(&basePtr[baseAddr-alignPadding], 0, alignPadding);
        memcpy(&basePtr[baseAddr], buf, bufSz);
    }

    void BuilderBlock::grow(size_t minSz, size_t clearTo)
    {
        assert(!isFinalized());
        if(minSz > bufSz)
        {
            if(minSz > bufCap)
            {
                bufCap = std::max(minSz, bufCap << 1);
                char * newBuf = new char[bufCap];
                memcpy(newBuf, buf, bufSz);
                std::swap(buf, newBuf);
                delete[] newBuf;
            }

            if(bufSz < clearTo)
                memset(&buf[bufSz], 0, clearTo - bufSz);

            bufSz = minSz;
        }
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
        grow(pos + len, pos);
        memcpy(&buf[pos], ptr, len);
    }

    template <class T>
    void BuilderBlock::write(size_t pos, T const & val)
    {
        write(pos, &val, sizeof(T));
    }

    void BuilderBlock::writeOffset(size_t pos, BuilderBlock * target,
                                   size_t targetOffset)
    {
        // Write null placeholder offset
        write<int32_t>(pos, OffsetBase<int32_t>::NULL_OFFSET);
        
        if(target)
        {
            // Both blocks must have the same builder
            assert(builder == target->builder);

            // Request an address fixup
            target->addBackReference(this, pos, targetOffset);
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

    void BuilderBlock::appendOffset(BuilderBlock * target,
                                    size_t targetOffset)
    {
        writeOffset(bufSz, target, targetOffset);
    }

    void BuilderBlock::appendPadding(size_t toAlignment)
    {
        size_t targetSz = alignUp(bufSz, toAlignment);
        grow(targetSz, targetSz);
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

