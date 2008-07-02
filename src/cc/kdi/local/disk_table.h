//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/local/disk_table.h#1 $
//
// Created 2007/10/04
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_LOCAL_DISK_TABLE_H
#define KDI_LOCAL_DISK_TABLE_H

#include <kdi/table.h>
#include <boost/noncopyable.hpp>

#include <oort/record.h>
#include <string>

namespace kdi {
namespace local {

    /// A read-only implementation of a table served out of a file.
    class DiskTable;

    /// Pointer to a DiskTable
    typedef boost::shared_ptr<DiskTable> DiskTablePtr;

    /// Pointer to a const DiskTable
    typedef boost::shared_ptr<DiskTable const> DiskTableCPtr;

} // namespace local
} // namespace kdi

//----------------------------------------------------------------------------
// DiskTable
//----------------------------------------------------------------------------
class kdi::local::DiskTable
    : public kdi::Table,
      private boost::noncopyable
{
    std::string fn;
    oort::Record indexRec;

public:
    explicit DiskTable(std::string const & fn);

    virtual void set(strref_t row, strref_t column, int64_t timestamp,
                     strref_t value);

    virtual void erase(strref_t row, strref_t column, int64_t timestamp);

    virtual CellStreamPtr scan() const;
    virtual CellStreamPtr scan(ScanPredicate const & pred) const;

    virtual void sync() { /* nothing to do */ }
};


#endif // KDI_LOCAL_DISK_TABLE_H
