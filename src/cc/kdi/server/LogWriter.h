//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-11
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

#ifndef KDI_SERVER_LOGWRITER_H
#define KDI_SERVER_LOGWRITER_H

#include <kdi/server/CellBuffer.h>
#include <kdi/strref.h>

namespace kdi {
namespace server {

    class LogWriter
    {
    public:
        virtual ~LogWriter() {}

        /// Record a block of cells for the named table in the log
        virtual void writeCells(strref_t tableName, strref_t packedCells) = 0;

        /// Get the approximate size of the log on disk
        virtual size_t getDiskSize() const = 0;

        /// Make sure all cells written so far are durable on
        /// permanent storage
        virtual void sync() = 0;

        /// Close the log and return the path to the finished log
        /// file
        virtual std::string finish() = 0;
    };

} // namespace server
} // namespace kdi

#endif // KDI_SERVER_LOGWRITER_H
