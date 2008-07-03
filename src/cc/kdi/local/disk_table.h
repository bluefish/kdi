//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-04
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
