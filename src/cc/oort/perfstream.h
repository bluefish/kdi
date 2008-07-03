//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-01-22
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

#ifndef OORT_PERFSTREAM_H
#define OORT_PERFSTREAM_H

#include "recordstream.h"
#include "warp/perfvar.h"
#include <string>

namespace oort
{
    class PerformanceRecordStream;
}

//----------------------------------------------------------------------------
// PerformanceRecordStream
//----------------------------------------------------------------------------
class oort::PerformanceRecordStream : public oort::RecordStream
{
    RecordStreamHandle stream;
    warp::PerformanceInteger recordsIn;
    warp::PerformanceInteger recordsOut;

public:
    /// Construct a PerformanceRecordStream wrapper over the given
    /// RecordStream handle.  The name corresponds to the name of the
    /// stream.  Register with the common PerformanceLog.
    PerformanceRecordStream(RecordStreamHandle const & s,
                            std::string const & name);

    /// Construct a PerformanceRecordStream wrapper over the given
    /// RecordStream handle.  The name corresponds to the name of the
    /// stream.  Register with the given PerformanceLog.
    PerformanceRecordStream(RecordStreamHandle const & s,
                            std::string const & name,
                            warp::PerformanceLog * log);


    // RecordStream API
    virtual void put(Record const & r);
    virtual bool get(Record & r);
};


#endif // OORT_PERFSTREAM_H
