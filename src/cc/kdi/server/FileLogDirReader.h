//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-05
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

#ifndef KDI_SERVER_FILELOGDIRREADER_H
#define KDI_SERVER_FILELOGDIRREADER_H

#include <kdi/server/LogDirReader.h>

namespace kdi {
namespace server {

    class FileLogDirReader
        : public LogDirReader
    {
    public:
        /// Read a log directory and return an iterator over
        /// LogReaders for logs in the directory.  The logs will be
        /// presented in order from oldest to newest.
        virtual std::auto_ptr<Iterator>
        readLogDir(std::string const & logDir) const;
    };

} // namespace server
} // namespace kdi

#endif // KDI_SERVER_FILELOGDIRREADER_H
