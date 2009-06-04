//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-09
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

#ifndef KDI_RPC_PACKEDCELLWRITER_H
#define KDI_RPC_PACKEDCELLWRITER_H

#include <warp/pack.h>
#include <warp/vint.h>
#include <warp/hashtable.h>
#include <warp/hsieh_hash.h>
#include <warp/adler.h>
#include <warp/string_range.h>
#include <boost/noncopyable.hpp>
#include <boost/scoped_array.hpp>
#include <algorithm>

namespace kdi {
namespace rpc {

    // PackedCellsV0:
    //   magic / 4            'CPk0'
    //   checksum / 4         Adler32 of from &skip to END
    //   skip / vint          Data between header end and start of first Cell
    //
    // Cell:  (backward offsets relative to beginning of Cell)
    //   { row,               Offset backward to row VString
    //     column,            Offset backward to column VString
    //     value,             Offset backward to value VString, or 0
    //     skip               Data between end of this Cell and start of next
    //   } / vquad            
    //   dt / vsint           Difference from last Cell time
    //
    // VString:
    //   length / vint        Length of string
    //   data / length        String data

    /// Write a sequence of cells in PackedCellsV0 format
    class PackedCellWriterV0;

    typedef PackedCellWriterV0 PackedCellWriter;

} // namespace rpc
} // namespace kdi


//----------------------------------------------------------------------------
// PackedCellWriterV0
//----------------------------------------------------------------------------
class kdi::rpc::PackedCellWriterV0
    : private boost::noncopyable
{
public:
    PackedCellWriterV0() :
        buf(),
        strings(0.75, StrHash(buf), StrEq(buf))
    {
        reset();
    }

    /// Append a positive (non-erasure) Cell to the output
    void append(warp::strref_t row, warp::strref_t column,
                int64_t timestamp, warp::strref_t value)
    {
        uint32_t totalOffset = 0;
        uint32_t r,c,v;

        prepareString(row,    r, totalOffset);
        prepareString(column, c, totalOffset);
        prepareString(value,  v, totalOffset);

        finishLastCell(totalOffset);
        
        if(!r) r = writeString(row);
        if(!c) c = writeString(column);
        if(!v) v = writeString(value);

        prepareNextCell(r, c, timestamp, v);
    }

    /// Append an erasure Cell to the output
    void appendErasure(warp::strref_t row, warp::strref_t column,
                       int64_t timestamp)
    {
        uint32_t totalOffset = 0;
        uint32_t r,c;

        prepareString(row,    r, totalOffset);
        prepareString(column, c, totalOffset);

        finishLastCell(totalOffset);
        
        if(!r) r = writeString(row);
        if(!c) c = writeString(column);

        prepareNextCell(r, c, timestamp, buf.size());
    }

    /// Finish building the packed cell block.  This should only be
    /// called once.
    void finish()
    {
        finishLastCell(0);
        uint32_t magic = WARP_PACK4('C','P','k','0');
        uint32_t checksum = warp::adler32(buf.addr(8), buf.size() - 8);
        memcpy(buf.addr(0), &magic, 4);
        memcpy(buf.addr(4), &checksum, 4);
    }

    /// Reset the writer for a new block.
    void reset()
    {
        buf.clear();
        strings.clear();
        lastTime = 0;
        nCells = 0;
    }

    /// Get the packed cell block from the last call to finish().
    warp::StringRange getPacked() const
    {
        return warp::StringRange(buf.addr(0), buf.addr(buf.size()));
    }

    /// Get the approximate size of the packed result assuming no more
    /// cells are to be added.
    size_t getDataSize() const
    {
        return buf.size() + warp::vint::MAX_QUAD_SIZE;
    }

    /// Get the number of cells added so far.
    size_t getCellCount() const
    {
        return nCells;
    }

    /// Get the last row added.  Only valid when getCellCount() > 0.
    warp::StringRange getLastRow() const
    {
        return getString(lastBase - lastOffsets.row);
    }

    /// Get the last column added.  Only valid when getCellCount() > 0.
    warp::StringRange getLastColumn() const
    {
        return getString(lastBase - lastOffsets.column);
    }

    /// Get the last timestamp added.  Only valid when getCellCount() > 0.
    int64_t getLastTimestamp() const
    {
        return lastTime;
    }

private:
    class Buf
    {
        boost::scoped_array<char> buf;
        uint32_t cap;
        uint32_t len;
        
    public:
        Buf() : cap(0), len(0) {}
        
        void reserve(uint32_t sz)
        {
            if(len + sz > cap)
            {
                cap = std::max(cap * 2, std::max(len+sz, 4u<<10));
                boost::scoped_array<char> buf2(new char[cap]);
                memcpy(buf2.get(), buf.get(), len);
                buf.swap(buf2);
            }
        }

        void clear()
        {
            len = 0;
        }

        char const * addr(uint32_t pos) const { return &buf[pos]; }
        char * addr(uint32_t pos) { return &buf[pos]; }

        char * end() { return &buf[len]; }
        void end(char * p) { len = p - buf.get(); }

        uint32_t size() const { return len; }
    };

    struct StrHash
    {
        Buf const & buf;
        StrHash(Buf const & buf) : buf(buf) {}

        uint32_t operator()(uint32_t pos) const
        {
            char const * p = buf.addr(pos);
            uint32_t len;
            p += warp::vint::decode(p, len);
            return warp::hsieh_hash(p, len);
        }

        uint32_t operator()(warp::strref_t s) const
        {
            return warp::hsieh_hash(s.begin(), s.size());
        }
    };

    struct StrEq
    {
        Buf const & buf;
        StrEq(Buf const & buf) : buf(buf) {}

        inline warp::StringRange get(uint32_t pos) const
        {
            char const * p = buf.addr(pos);
            uint32_t len;
            p += warp::vint::decode(p, len);
            return warp::StringRange(p, p+len);
        }

        bool operator()(uint32_t a, warp::strref_t b) const
        {
            return get(a) == b;
        }

        bool operator()(warp::strref_t a, uint32_t b) const
        {
            return a == get(b);
        }
    };

    typedef warp::HashTable<uint32_t, StrHash, uint32_t, StrEq> set_t;

    struct Offsets
    {
        uint32_t row;
        uint32_t column;
        uint32_t value;
        uint32_t skip;
        operator uint32_t *() { return &row; }
    };

private:
    Buf buf;
    set_t strings;

    Offsets lastOffsets;
    int64_t lastTime;
    int64_t deltaTime;
    size_t lastBase;
    size_t nCells;

private:
    warp::StringRange getString(uint32_t pos) const
    {
        char const * p = buf.addr(pos);
        size_t len;
        p += warp::vint::decode(p, len);
        return warp::StringRange(p, p + len);
    }
    
    uint32_t findString(warp::strref_t s) const
    {
        set_t::const_iterator i = strings.find(s);
        if(i != strings.end())
            return *i;
        else
            return 0;
    }

    void prepareString(warp::strref_t s, uint32_t & pos, uint32_t & sz)
    {
        pos = findString(s);
        if(!pos)
            sz += warp::vint::getEncodedLength(s.size()) + s.size();
    }

    uint32_t writeString(warp::strref_t s)
    {
        uint32_t pos = buf.size();
        buf.reserve(warp::vint::MAX_VINT_SIZE + s.size());
        char * p = buf.end();
        p += warp::vint::encode(s.size(), p);
        memcpy(p, s.begin(), s.size());
        buf.end(p + s.size());
        strings.insert(pos);
        return pos;
    }

    void finishLastCell(uint32_t dataOffset)
    {
        if(!buf.size())
        {
            // No cells yet.  Reserve space for the header, then write
            // the first cell offset.
            buf.reserve(8 + warp::vint::getEncodedLength(dataOffset));
            char * p = buf.addr(8);
            p += warp::vint::encode(dataOffset, p);
            buf.end(p);
        }
        else
        {
            // Set the last cell's skip field and write the last cell.
            lastOffsets.skip = dataOffset;
            buf.reserve(warp::vint::MAX_QUAD_SIZE + warp::vint::MAX_VINT_SIZE);
            char * p = buf.end();
            p += warp::vint::encodeQuad(lastOffsets, p);
            p += warp::vint::encodeSigned(deltaTime, p);
            buf.end(p);
        }
    }

    void prepareNextCell(uint32_t rowPos, uint32_t columnPos, int64_t timestamp,
                         uint32_t valuePos)
    {
        lastBase = buf.size();
        lastOffsets.row = lastBase - rowPos;
        lastOffsets.column = lastBase - columnPos;
        lastOffsets.value = lastBase - valuePos;
        lastOffsets.skip = 0;
        deltaTime = timestamp - lastTime;
        lastTime = timestamp;
        ++nCells;
    }
};


#endif // KDI_RPC_PACKEDCELLWRITER_H
