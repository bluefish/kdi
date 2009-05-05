//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-03
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

#include <kdi/server/FileLogWriter.h>
#include <kdi/server/FileLog.h>
#include <warp/adler.h>
#include <ex/exception.h>

using namespace kdi::server;
using namespace ex;

//----------------------------------------------------------------------------
// FileLogWriter
//----------------------------------------------------------------------------
FileLogWriter::FileLogWriter(warp::FilePtr const & fp) :
    fp(fp)
{
    FileLogHeader h;
    h.magic = FileLogHeader::MAGIC;
    h.version = 0;

    size_t sz = fp->write(&h, sizeof(h));
    if(sz != sizeof(h))
        raise<IOError>("failed to write log header");
}

void FileLogWriter::writeCells(strref_t tableName, strref_t packed)
{
    if(tableName.size() > FileLogEntryHeaderV0::MAX_NAME_LEN)
        raise<ValueError>("table name is too long");

    if(packed.size() > FileLogEntryHeaderV0::MAX_DATA_LEN)
        raise<ValueError>("cell data is too large");

    FileLogEntryHeaderV0 hdr;
    hdr.magic = FileLogEntryHeaderV0::MAGIC;
    hdr.nameLen = tableName.size();
    hdr.dataLen = packed.size();

    warp::Adler32 adler;
    adler.update(&hdr.nameLen, 8);
    adler.update(tableName.begin(), tableName.size());
    hdr.checksum = adler.get();
    
    if(sizeof(hdr) != fp->write(&hdr, sizeof(hdr)))
        raise<IOError>("failed to write log entry header");
    if(tableName.size() != fp->write(tableName.begin(), tableName.size()))
        raise<IOError>("failed to write log table name");
    if(packed.size() != fp->write(packed.begin(), packed.size()))
        raise<IOError>("failed to write log cells");
}

size_t FileLogWriter::getDiskSize() const
{
    return fp->tell();
}

void FileLogWriter::sync()
{
    fp->flush();
}

std::string FileLogWriter::finish()
{
    std::string r;
    r = fp->getName();
    fp->close();
    return r;
}
