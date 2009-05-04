//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-04
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

#include <kdi/server/FileLogReader.h>
#include <kdi/server/FileLog.h>
#include <warp/adler.h>

using namespace kdi::server;

//----------------------------------------------------------------------------
// FileLogReader
//----------------------------------------------------------------------------
FileLogReaderV0::FileLogReaderV0(warp::FilePtr const & fp) :
    fp(fp), haveNext(false)
{
}

bool FileLogReaderV0::next()
{
    if(!haveNext && S_OK != getNextHeader())
        return false;

    haveNext = false;
    tableName.clear();
    packedCells.clear();

    tableName.resize(nextHdr.nameLen);
    if(nextHdr.nameLen != fp->read(&tableName[0], nextHdr.nameLen))
        return false;

    warp::Adler32 adler;
    adler.update(&nextHdr.nameLen, 8);
    adler.update(&tableName[0], tableName.size());
    if(nextHdr.checksum != adler.get())
        return false;

    packedCells.resize(nextHdr.dataLen);
    if(nextHdr.dataLen != fp->read(&packedCells[0], nextHdr.dataLen))
        return false;

    if(getNextHeader() == S_INVALID)
        return false;

    return true;
}

warp::StringRange FileLogReaderV0::getTableName() const
{
    return tableName;
}

warp::StringRange FileLogReaderV0::getPackedCells() const
{
    return packedCells;
}

FileLogReaderV0::HeaderStatus FileLogReaderV0::getNextHeader()
{
    haveNext = false;

    size_t readSz = fp->read(&nextHdr, sizeof(nextHdr));
    if(!readSz)
        return S_EOF;

    if(readSz != sizeof(nextHdr))
        return S_INVALID;

    if(nextHdr.magic != FileLogEntryHeaderV0::MAGIC)
        return S_INVALID;

    if(nextHdr.nameLen > FileLogEntryHeaderV0::MAX_NAME_LEN)
        return S_INVALID;

    if(nextHdr.dataLen > FileLogEntryHeaderV0::MAX_DATA_LEN)
        return S_INVALID;

    haveNext = true;
    return S_OK;
}
