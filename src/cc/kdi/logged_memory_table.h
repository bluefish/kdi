//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-16
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

#ifndef KDI_LOGGED_MEMORY_TABLE_H
#define KDI_LOGGED_MEMORY_TABLE_H

#include <kdi/memory_table.h>
#include <boost/scoped_ptr.hpp>

namespace kdi {
    
    /// A Table implementation in main memory, where every mutation is
    /// logged to disk.  The table can be re-created by replaying the
    /// log.
    class LoggedMemoryTable;

    /// Pointer to a LoggedMemoryTable
    typedef boost::shared_ptr<LoggedMemoryTable> LoggedMemoryTablePtr;

    /// Pointer to a const LoggedMemoryTable
    typedef boost::shared_ptr<LoggedMemoryTable const> LoggedMemoryTableCPtr;
    
} // namespace kdi

//----------------------------------------------------------------------------
// LoggedMemoryTable
//----------------------------------------------------------------------------
class kdi::LoggedMemoryTable
    : public kdi::MemoryTable
{
    typedef kdi::MemoryTable super;
    
    class Log;
    boost::scoped_ptr<Log> log;

protected:
    explicit LoggedMemoryTable(std::string const & logFn,
                               bool filterErasures);

public:
    /// Create a LoggedMemoryTable on the given log file.  If the log
    /// file exists, it will be opened and replayed to populate the
    /// table.  Then the log is reopened in append mode to become the
    /// continuing log for the table.
    static LoggedMemoryTablePtr create(std::string const & logFn,
                                       bool filterErasures);

    virtual void set(strref_t row, strref_t column, int64_t timestamp,
                     strref_t value);

    virtual void erase(strref_t row, strref_t column, int64_t timestamp);

    virtual void sync();
};

#endif // KDI_LOGGED_MEMORY_TABLE_H
