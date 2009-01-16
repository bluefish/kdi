//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-04
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

#ifndef KDI_LOCAL_DISK_TABLE_H
#define KDI_LOCAL_DISK_TABLE_H

#include <kdi/table.h>
#include <boost/noncopyable.hpp>

#include <oort/record.h>
#include <warp/interval.h>
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
    size_t dataSize;

public:
    explicit DiskTable(std::string const & fn);

    /// Load the index Record from the given file
    /// @returns offset to index from start of file
    static off_t loadIndex(std::string const & fn, oort::Record & r);

    virtual void set(strref_t row, strref_t column, int64_t timestamp,
                     strref_t value);

    virtual void erase(strref_t row, strref_t column, int64_t timestamp);

    using Table::scan;
    virtual CellStreamPtr scan(ScanPredicate const & pred) const;

    virtual void sync() { /* nothing to do */ }

    /// Scan the index of the fragment within the given row range,
    /// returning (row, incremental-size) pairs.  The incremental size
    /// approximates the on-disk size of the cells between the last
    /// returned row key (or the given lower bound) and the current
    /// row key.
    flux::Stream< std::pair<std::string, size_t> >::handle_t
    scanIndex(warp::Interval<std::string> const & rows) const;

    size_t getIndexSize() const { return indexRec.getLength(); }
    size_t getDataSize() const { return dataSize; }
};


#endif // KDI_LOCAL_DISK_TABLE_H
