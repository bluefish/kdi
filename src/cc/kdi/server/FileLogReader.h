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

#ifndef KDI_SERVER_FILELOGREADER_H
#define KDI_SERVER_FILELOGREADER_H

#include <kdi/server/LogReader.h>
#include <kdi/server/FileLog.h>
#include <warp/file.h>
#include <vector>

namespace kdi {
namespace server {

    class FileLogReaderV0;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// FileLogReaderV0
//----------------------------------------------------------------------------
class kdi::server::FileLogReaderV0
    : public kdi::server::LogReader
{
public:
    explicit FileLogReaderV0(warp::FilePtr const & fp);

    virtual bool next();
    virtual warp::StringRange getTableName() const;
    virtual warp::StringRange getPackedCells() const;

private:
    enum HeaderStatus {
        S_OK,
        S_EOF,
        S_INVALID,
    };

    HeaderStatus getNextHeader();

private:
    warp::FilePtr fp;
    std::vector<char> tableName;
    std::vector<char> packedCells;
    FileLogEntryHeaderV0 nextHdr;
    bool haveNext;
};

#endif // KDI_SERVER_FILELOGREADER_H
