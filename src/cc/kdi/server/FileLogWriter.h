//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-01
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

#ifndef KDI_SERVER_FILELOGWRITER_H
#define KDI_SERVER_FILELOGWRITER_H

#include <kdi/server/LogWriter.h>
#include <warp/file.h>

namespace kdi {
namespace server {

    class FileLogWriter;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// FileLogWriter
//----------------------------------------------------------------------------
class kdi::server::FileLogWriter
    : public LogWriter
{
public:
    explicit FileLogWriter(warp::FilePtr const & fp);

    /// Record a block of cells for the named table in the log
    virtual void writeCells(strref_t tableName,
                            CellBufferCPtr const & cells);

    /// Get the approximate size of the log on disk
    virtual size_t getDiskSize() const;

    /// Make sure all cells written so far are durable on
    /// permanent storage
    virtual void sync();

    /// Close the log and return the path to the finished log
    /// file
    virtual std::string finish();

private:
    warp::FilePtr fp;
};


#endif // KDI_SERVER_FILELOGWRITER_H
