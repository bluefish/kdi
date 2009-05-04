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

#ifndef KDI_SERVER_LOGREADER_H
#define KDI_SERVER_LOGREADER_H

#include <warp/string_range.h>

namespace kdi {
namespace server {

    class LogReader
    {
    public:
        virtual ~LogReader() {}

        /// Move the LogReader to the next block in the log file.
        /// Return true if such a block exists.
        virtual bool next() = 0;

        /// Get the name of the table for the current log block.  Only
        /// valid if the last call to next() returned true.
        virtual warp::StringRange getTableName() const = 0;

        /// Get the packed cells for the current log block.  Only
        /// valid if the last call to next() returned true.
        virtual warp::StringRange getPackedCells() const = 0;
    };

} // namespace server
} // namespace kdi

#endif // KDI_SERVER_LOGREADER_H
