//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-08-21
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

#ifndef KDI_HYPER_TABLE_H
#define KDI_HYPER_TABLE_H

#include <kdi/table.h>
#include <Hypertable/Lib/Table.h>
#include <Hypertable/Lib/TableMutator.h>

namespace kdi {
namespace hyper {

    class Table;

} // namespace hyper
} // namespace kdi

//----------------------------------------------------------------------------
// Table
//----------------------------------------------------------------------------
class kdi::hyper::Table
    : public kdi::Table
{
    ::Hypertable::TablePtr table;
    ::Hypertable::TableMutatorPtr mutator;

public:
    explicit Table(std::string const & tableName);

    virtual void set(strref_t row, strref_t column, int64_t timestamp,
                     strref_t value);
    virtual void erase(strref_t row, strref_t column, int64_t timestamp);
    virtual void sync();
    virtual CellStreamPtr scan(ScanPredicate const & pred) const;
};

#endif // KDI_HYPER_TABLE_H
