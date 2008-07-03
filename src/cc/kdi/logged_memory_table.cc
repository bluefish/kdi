//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-16
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

#include <kdi/logged_memory_table.h>
#include <warp/string_data.h>
#include <oort/recordbuilder.h>
#include <oort/buildstream.h>
#include <boost/noncopyable.hpp>

using namespace kdi;
using namespace warp;
using namespace ex;
using namespace std;
using namespace oort;


//----------------------------------------------------------------------------
// Notes
//----------------------------------------------------------------------------

// Logged MemoryTable
//   extend MemoryTable, make all writes go to disk first.  use log
//   replay on load.  log format may require that multiple tables
//   operate from the same log file for write performance (like
//   BigTable).  this detail can be postponed.
//   in the event of multiple tables sharing the same log file, we'd
//   probably want to periodically snip the log and start a new one.
//   this could be done by starting a new common log and scheduling
//   all the tables using the old log for serialization.  once they're
//   all done, the old log can be deleted.


//----------------------------------------------------------------------------
// Log format
//----------------------------------------------------------------------------
namespace {

    struct LogCell
    {
        enum {
            TYPECODE = WARP_PACK4('L','o','g','C'),
            VERSION = 0,
            ALIGNMENT = 8,
            FLAGS = 0,
        };

        StringOffset row;
        StringOffset column;
        int64_t timestamp;
        StringOffset value;     // null means erasure
    };

}

//----------------------------------------------------------------------------
// LoggedMemoryTable::Log
//----------------------------------------------------------------------------
class LoggedMemoryTable::Log
    : public boost::noncopyable
{
    RecordBuilder b;
    BuildStreamHandle out;

public:
    explicit Log(string const & logFn) :
        out(makeBuildStream<LogCell>(32<<10))
    {
        out->pipeTo(appendStream(logFn));
    }

    void set(strref_t row, strref_t column, int64_t timestamp,
             strref_t value)
    {
        b << (*b.subblock(4) << StringData::wrap(row))
          << (*b.subblock(4) << StringData::wrap(column))
          << timestamp
          << (*b.subblock(4) << StringData::wrap(value));
        out->put(b);
    }

    void erase(strref_t row, strref_t column, int64_t timestamp)
    {
        b << (*b.subblock(4) << StringData::wrap(row))
          << (*b.subblock(4) << StringData::wrap(column))
          << timestamp;
        b.appendOffset(0);
        out->put(b);
    }

    void flush()
    {
        out->flush();
    }
};


//----------------------------------------------------------------------------
// LoggedMemoryTable
//----------------------------------------------------------------------------
LoggedMemoryTable::LoggedMemoryTable(string const & logFn,
                                     bool filterErasures) :
    MemoryTable(filterErasures)
{
    // Replay log file if it exists
    RecordStreamHandle input;
    try {
        input = inputStream(logFn);
    }
    catch(IOError const &) {
        // pass
    }
    if(input)
    {
        // Opened a log file -- replay it to populate table
        Record x;
        while(input->get(x))
        {
            // Update table with cell.  A null value field indicates
            // an erasure cell.
            LogCell const * cell = x.as<LogCell>();
            if(cell->value)
            {
                // Set cell
                super::set(cell->row, cell->column,
                           cell->timestamp, cell->value);
            }
            else
            {
                // Erase cell
                super::erase(cell->row, cell->column,
                             cell->timestamp);
            }
        }
        // Close input stream
        input.reset();
    }

    // Reopen log for append
    log.reset(new Log(logFn));
}

LoggedMemoryTablePtr LoggedMemoryTable::create(string const & logFn,
                                               bool filterErasures)
{
    LoggedMemoryTablePtr p(new LoggedMemoryTable(logFn, filterErasures));
    return p;
}

void LoggedMemoryTable::set(strref_t row, strref_t column,
                            int64_t timestamp, strref_t value)
{
    // Commit SET to table.  Do this first so the base class has a
    // chance to throw an exception before we commit to the log.
    super::set(row, column, timestamp, value);

    // Write a SET to the log.
    log->set(row, column, timestamp, value);
}

void LoggedMemoryTable::erase(strref_t row, strref_t column,
                              int64_t timestamp)
{
    // Commit ERASE to table.  Do this first so the base class has a
    // chance to throw an exception before we commit to the log.
    super::erase(row, column, timestamp);

    // Write an ERASE to the log
    log->erase(row, column, timestamp);
}

void LoggedMemoryTable::sync()
{
    // Sync MemoryTable first so it also has a chance to report
    // errors.  It won't because MemoryTable::sync() is a no-op, but
    // just in case that changes someday...
    super::sync();

    // Flush log to disk
    log->flush();
}

