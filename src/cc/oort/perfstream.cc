//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/oort/perfstream.cc#1 $
//
// Created 2007/01/22
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "perfstream.h"
#include "warp/perflog.h"

using namespace oort;
using namespace warp;
using namespace ex;
using namespace std;

//----------------------------------------------------------------------------
// PerformanceRecordStream
//----------------------------------------------------------------------------
PerformanceRecordStream::PerformanceRecordStream(
    RecordStreamHandle const & s, string const & name) :
    stream(s),
    recordsIn(string("stream input ") + name + " (records)"),
    recordsOut(string("stream output ") + name + " (records)")
{
    if(!stream)
        raise<ValueError>("null stream");

    PerformanceLog & log = PerformanceLog::getCommon();
    log.addItem(&recordsIn);
    log.addItem(&recordsOut);
}

PerformanceRecordStream::PerformanceRecordStream(
    RecordStreamHandle const & s, string const & name,
    PerformanceLog * log) :
    stream(s),
    recordsIn(string("stream input ") + name + " (records)"),
    recordsOut(string("stream output ") + name + " (records)")
{
    if(!stream)
        raise<ValueError>("null stream");

    if(!log)
        raise<ValueError>("null performance log");

    log->addItem(&recordsIn);
    log->addItem(&recordsOut);
}

void PerformanceRecordStream::put(Record const & r)
{
    recordsOut += 1;
    stream->put(r);
}

bool PerformanceRecordStream::get(Record & r)
{
    if(stream->get(r))
    {
        recordsIn += 1;
        return true;
    }
    else
    {
        return false;
    }
}
