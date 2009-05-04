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

#ifndef KDI_RPC_PACKEDCELLREADER_H
#define KDI_RPC_PACKEDCELLREADER_H

#include <kdi/strref.h>
#include <kdi/CellKey.h>
#include <warp/vint.h>
#include <warp/adler.h>
#include <warp/pack.h>
#include <boost/noncopyable.hpp>
#include <string.h>

//#include <iostream>
//using namespace std;

namespace kdi {
namespace rpc {

    class PackedCellReaderV0;

    typedef PackedCellReaderV0 PackedCellReader;

} // namespace rpc
} // namespace kdi

//----------------------------------------------------------------------------
// PackedCellReaderV0
//----------------------------------------------------------------------------
class kdi::rpc::PackedCellReaderV0
    : private boost::noncopyable
{
public:
    PackedCellReaderV0() : cellBase(0) {}

    explicit PackedCellReaderV0(strref_t packed)
    {
        reset(packed);
    }

    /// Reset the reader with the given cell buffer.  The reader will
    /// not copy the buffer, so the external memory range must remain
    /// valid until the next call to reset().
    void reset(strref_t packed)
    {
        buf = packed;
        cellBase = 0;
        timestamp = 0;
    }

    /// Return true iff the current buffer's magic number is correct.
    bool verifyMagic() const
    {
        if(buf.size() < 4)
            return false;

        uint32_t magic = WARP_PACK4('C','P','k','0');
        return 0 == memcmp(&magic, buf.begin(), 4);
    }

    /// Return true iff the current buffer's checksum is correct.
    bool verifyChecksum() const
    {
        if(buf.size() < 8)
            return false;

        uint32_t checksum = warp::adler32(buf.begin() + 8, buf.size() - 8);
        return 0 == memcmp(&checksum, buf.begin() + 4, 4);
    }

    /// Return true iff all offsets point to well-formed data within
    /// the current buffer.
    bool verifyOffsets() const
    {
        if(buf.size() < 9)
            return false;

        char const * p = buf.begin() + 8;
        char const * end = buf.end();

        {
            size_t skip;
            size_t sz = warp::vint::decodeSafe(p, end-p, skip);
            if(!sz)
                return false;
            p += sz + skip;
        }

        while(p != end)
        {
            if(p > end)
                return false;

            Offsets off;
            char const * base = p;
            size_t sz = warp::vint::decodeQuadSafe(p, end-p, off);
            if(!sz)
                return false;
            p += sz;

            int64_t deltaTime;
            sz = warp::vint::decodeSignedSafe(p, end-p, deltaTime);
            if(!sz)
                return false;
            p += sz;

            if(!verifyString(base - off.row))
                return false;
            if(!verifyString(base - off.column))
                return false;
            if(off.value && !verifyString(base - off.value))
                return false;

            p += off.skip;
        }

        return true;
    }

    /// Advance reader to the next cell.  Return true if a cell is
    /// available or false if the reader has reached the end of the
    /// buffer.
    bool next()
    {
        if(!cellBase)
        {
            if(buf.size() < 9)
                return false;
            
            cellBase = buf.begin() + 8;
            size_t skip;
            cellBase += warp::vint::decode(cellBase, skip);
            cellBase += skip;
            //cerr << "Start, skip " << skip << endl;
        }
        else
        {
            //cerr << "Continue, skip " << offsets.skip << endl;
            cellBase = cellEnd + offsets.skip;
        }

        if(cellBase == buf.end())
        {
            //cerr << "Done" << endl;
            return false;
        }

        int64_t deltaTime;
        cellEnd = cellBase;
        
        //cerr << "Base: " << (cellBase - buf.begin()) << endl;

        cellEnd += warp::vint::decodeQuad(cellEnd, offsets);
        cellEnd += warp::vint::decodeSigned(cellEnd, deltaTime);
        timestamp += deltaTime;

        //cerr << "End: " << (cellEnd - buf.begin()) << endl;
        //cerr << "  row: " << offsets.row << endl;
        //cerr << "  col: " << offsets.column << endl;
        //cerr << "  val: " << offsets.value << endl;
        //cerr << "  ts : " << timestamp << endl;
        //cerr << "  dt : " << deltaTime << endl;

        return true;
    }

    void getKey(CellKeyRef & key) const
    {
        key.setRow(getRow());
        key.setColumn(getColumn());
        key.setTimestamp(getTimestamp());
    }

    CellKeyRef getKey() const
    {
        CellKeyRef r;
        getKey(r);
        return r;
    }

    /// Get the row of the current cell.  Only valid if the last call
    /// to next() returned true.
    warp::StringRange getRow() const
    {
        return getString(cellBase - offsets.row);
    }

    /// Get the column of the current cell.  Only valid if the last
    /// call to next() returned true.
    warp::StringRange getColumn() const
    {
        return getString(cellBase - offsets.column);
    }

    /// Get the value of the current cell.  Only valid if the last
    /// call to next() returned true.  If the cell is an erasure, the
    /// value will be empty.
    warp::StringRange getValue() const
    {
        if(offsets.value)
            return getString(cellBase - offsets.value);
        else
            return warp::StringRange();
    }

    /// Get the timestamp of the current cell.  Only valid if the last
    /// call to next() returned true.
    int64_t getTimestamp() const
    {
        return timestamp;
    }
    
    /// Return true if the current cell is an erasure.  Only valid if
    /// the last call to next() returned true.
    bool isErasure() const
    {
        return offsets.value == 0;
    }

private:
    struct Offsets
    {
        uint32_t row;
        uint32_t column;
        uint32_t value;
        uint32_t skip;
        operator uint32_t *() { return &row; }
    };

private:
    warp::StringRange buf;
    char const * cellBase;
    char const * cellEnd;
    Offsets offsets;
    int64_t timestamp;

    static inline warp::StringRange getString(char const * p)
    {
        size_t len;
        p += warp::vint::decode(p, len);
        return warp::StringRange(p, p+len);
    }

    inline bool verifyString(char const * p) const
    {
        if(p < buf.begin() || p >= buf.end())
            return false;
        size_t len;
        size_t sz = warp::vint::decodeSafe(p, buf.end() - p, len);
        if(!sz)
            return false;
        if(p + sz + len > buf.end() || p + sz + len < p + sz)
            return false;
        return true;
    }
};


#endif // KDI_RPC_PACKEDCELLREADER_H
