//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-12
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
