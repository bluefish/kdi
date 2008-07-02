//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/oort/perfstream.h#1 $
//
// Created 2007/01/22
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
