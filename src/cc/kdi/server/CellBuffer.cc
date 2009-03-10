//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-05
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

#include <kdi/server/CellBuffer.h>
#include <kdi/server/errors.h>
#include <kdi/rpc/PackedCellReader.h>

using namespace kdi;
using namespace kdi::server;
using kdi::rpc::PackedCellReader;

//----------------------------------------------------------------------------
// CellBuffer
//----------------------------------------------------------------------------
CellBuffer::CellBuffer(strref_t data_)
{
    PackedCellReader reader(data_);
    
    if(!reader.verifyMagic())
        throw BadMagicError();
    
    if(!reader.verifyChecksum())
        throw BadChecksumError();
    
    if(!reader.verifyOffsets())
        throw CellDataError();

    nCells = 0;
    if(reader.next())
    {
        ++nCells;
        CellKeyRef last = reader.getKey();
        while(reader.next())
        {
            ++nCells;
            CellKeyRef key = reader.getKey();
            if(!(last < key))
                throw BadOrderError();
            last = key;
        }
    }

    data.insert(data.end(), data_.begin(), data_.end());
}

void CellBuffer::getRows(std::vector<warp::StringRange> & rows) const
{
    PackedCellReader reader(data);
    
    rows.clear();
    while(reader.next())
    {
        if(rows.empty() || rows.back() != reader.getRow())
            rows.push_back(reader.getRow());
    }
}
