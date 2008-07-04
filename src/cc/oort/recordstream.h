//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-03-08
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef OORT_RECORDSTREAM_H
#define OORT_RECORDSTREAM_H

#include <oort/record.h>
#include <flux/stream.h>
#include <string>

namespace oort
{
    typedef flux::Stream<Record> RecordStream;
    typedef RecordStream::handle_t RecordStreamHandle;

    /// Open a RecordStream by name.  The \c flags parameter is the
    /// same as for File::open().
    extern RecordStreamHandle openStream(std::string const & uriFn, int flags);

    /// Alias for openStream(uriFn, O_RDONLY).
    extern RecordStreamHandle inputStream(std::string const & uriFn);

    /// Alias for openStream(uriFn, O_WRONLY | O_CREAT | O_TRUNC).
    extern RecordStreamHandle outputStream(std::string const & uriFn);

    /// Alias for openStream(uriFn, O_WRONLY | O_CREAT | O_APPEND).
    extern RecordStreamHandle appendStream(std::string const & uriFn);

    /// Stream open function prototype -- mode is same as used by File::open.
    typedef RecordStreamHandle (OpenStreamFunc)(std::string const & uri, int mode);

    /// Register a handler for a particular scheme.
    extern void registerScheme(char const * name, OpenStreamFunc * func);
}

#endif // OORT_RECORDSTREAM_H
