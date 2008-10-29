//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-10-24
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

#ifndef KDI_HASH_HASHEDTABLE_H
#define KDI_HASH_HASHEDTABLE_H

#include <kdi/table.h>
#include <boost/noncopyable.hpp>
#include <vector>
#include <string>

namespace kdi {
namespace hash {

    class HashedTable;

} // namespace hash
} // namespace kdi

//----------------------------------------------------------------------------
// HashedTable
//----------------------------------------------------------------------------
class kdi::hash::HashedTable
    : public kdi::Table,
      private boost::noncopyable
{
    struct TableInfo
    {
        TablePtr table;
        bool isDirty;

        TableInfo(TablePtr const & table) :
            table(table), isDirty(false) {}
    };

    std::vector<TableInfo> tables;

    /// Pick a table for mutation based on the row key.  The
    /// associated TableInfo will have its dirty flag set.
    inline TablePtr const & pick(strref_t row);

public:
    /// Construct a HashedTable from vector of open Tables.  Requests
    /// will be distributed between the tables based on a hash of the
    /// row key.
    explicit HashedTable(std::vector<TablePtr> const & tables);

    virtual void set(strref_t row, strref_t column, int64_t timestamp,
                     strref_t value);
    virtual void erase(strref_t row, strref_t column, int64_t timestamp);
    virtual void insert(Cell const & x);
    virtual CellStreamPtr scan(ScanPredicate const & pred) const;
    virtual void sync();
    virtual RowIntervalStreamPtr scanIntervals() const;
};


#endif // KDI_HASH_HASHEDTABLE_H
