//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/local/local_table.h#1 $
//
// Created 2007/10/12
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_LOCAL_LOCAL_TABLE_H
#define KDI_LOCAL_LOCAL_TABLE_H

#include <kdi/table.h>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>

namespace kdi {
namespace local {

    class LocalTable;

    typedef boost::shared_ptr<LocalTable> LocalTablePtr;
    typedef boost::shared_ptr<LocalTable const> LocalTableCPtr;

} // namespace local
} // namespace kdi

//----------------------------------------------------------------------------
// LocalTable
//----------------------------------------------------------------------------
class kdi::local::LocalTable
    : public kdi::Table,
      private boost::noncopyable
{
    class Impl;
    boost::shared_ptr<Impl> impl;
    
public:
    explicit LocalTable(std::string const & tableDir);
    ~LocalTable();

    virtual void set(strref_t row, strref_t column, int64_t timestamp,
                     strref_t value);

    virtual void erase(strref_t row, strref_t column, int64_t timestamp);

    virtual CellStreamPtr scan() const;
    virtual CellStreamPtr scan(ScanPredicate const & pred) const;

    virtual void sync();

    /// Serialize data held in memory to a disk table.  This can be
    /// used to reduce the resident memory footprint of the table.
    /// This function schedules a serialization request and returns
    /// immediately.  The actual serialization is done in the
    /// background.
    void flushMemory();

    /// Perform a full compaction of serialized data on disk.  This
    /// function schedules a compaction and returns immediately.  The
    /// compaction happens in the background.
    void compactTable();

    /// Get an estimate of the current memory footprint of this table.
    size_t getMemoryUsage() const;
};

#endif // KDI_LOCAL_LOCAL_TABLE_H
