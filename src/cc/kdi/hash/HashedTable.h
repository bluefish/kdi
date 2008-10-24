//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/hash/HashedTable.h $
//
// Created 2008/10/24
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
    virtual CellStreamPtr scan() const;
    virtual CellStreamPtr scan(ScanPredicate const & pred) const;
    virtual void sync();
};


#endif // KDI_HASH_HASHEDTABLE_H
