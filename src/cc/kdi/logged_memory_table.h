//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/logged_memory_table.h#1 $
//
// Created 2007/10/16
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
