//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/oort/recordstream.h#1 $
//
// Created 2006/03/08
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef OORT_RECORDSTREAM_H
#define OORT_RECORDSTREAM_H

#include "record.h"
#include "flux/stream.h"
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
