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

#include <oort/perfstream.h>
#include <warp/perflog.h>

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
