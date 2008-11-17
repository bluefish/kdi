//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-11-16
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

#ifndef KDI_CELL_FIELD_H
#define KDI_CELL_FIELD_H

#include <kdi/cell.h>
#include <warp/string_range.h>
#include <algorithm>

namespace kdi {

    namespace field {

        class Row
        {
        public:
            typedef warp::StringRange value_t;
            static value_t get(Cell const & x) {
                return x.getRow();
            }
        };

        class Column
        {
        public:
            typedef warp::StringRange value_t;
            static value_t get(Cell const & x) {
                return x.getColumn();
            }
        };

        class ColumnFamily
        {
        public:
            typedef warp::StringRange value_t;
            static value_t get(Cell const & x) {
                return x.getColumnFamily();
            }
        };

        class ColumnQualifier
        {
        public:
            typedef warp::StringRange value_t;
            static value_t get(Cell const & x) {
                return x.getColumnQualifier();
            }
        };

        class Timestamp
        {
        public:
            typedef int64_t value_t;
            static value_t get(Cell const & x) {
                return x.getTimestamp();
            }
        };

        class Value
        {
        public:
            typedef warp::StringRange value_t;
            static value_t get(Cell const & x) {
                return x.getValue();
            }
        };

    } // namespace kdi::field


    //------------------------------------------------------------------------
    // CellField
    //------------------------------------------------------------------------
    template <class Field>
    class CellField
    {
    public:
        typedef typename Field::value_t value_t;
        value_t value;
        explicit CellField(value_t const & value) : value(value) {}
    };

    template <class Field>
    bool operator<(CellField<Field> const & a, Cell const & b)
    {
        return a.value < Field::get(b);
    }

    template <class Field>
    bool operator==(CellField<Field> const & a, Cell const & b)
    {
        return a.value == Field::get(b);
    }

    template <class Field>
    bool operator<(Cell const & a, CellField<Field> const & b)
    {
        return Field::get(a) < b.value;
    }

    template <class Field>
    bool operator==(Cell const & a, CellField<Field> const & b)
    {
        return Field::get(a) == b.value;
    }

    typedef CellField<field::Row>               CellRow;
    typedef CellField<field::Column>            CellColumn;
    typedef CellField<field::ColumnFamily>      CellColumnFamily;
    typedef CellField<field::ColumnQualifier>   CellColumnQualifier;
    typedef CellField<field::Timestamp>         CellTimestamp;
    typedef CellField<field::Value>             CellValue;


    //------------------------------------------------------------------------
    // CellFieldPrefix
    //------------------------------------------------------------------------
    template <class Field>
    class CellFieldPrefix
    {
    public:
        typedef typename Field::value_t value_t;
        value_t prefix;
        explicit CellFieldPrefix(value_t const & prefix) : prefix(prefix) {}
        value_t getPrefix(Cell const & x) const
        {
            value_t value = Field::get(x);
            size_t minSz = std::min(prefix.size(), value.size());
            return value_t(value.begin(), value.begin() + minSz);
        }
    };

    template <class Field>
    bool operator<(CellFieldPrefix<Field> const & a, Cell const & b)
    {
        return a.prefix < a.getPrefix(b);
    }

    template <class Field>
    bool operator==(CellFieldPrefix<Field> const & a, Cell const & b)
    {
        return a.prefix == a.getPrefix(b);
    }

    template <class Field>
    bool operator<(Cell const & a, CellFieldPrefix<Field> const & b)
    {
        return b.getPrefix(a) < b.prefix;
    }

    template <class Field>
    bool operator==(Cell const & a, CellFieldPrefix<Field> const & b)
    {
        return b.getPrefix(a) == b.prefix;
    }

    typedef CellFieldPrefix<field::Row>               CellRowPrefix;
    typedef CellFieldPrefix<field::Column>            CellColumnPrefix;
    typedef CellFieldPrefix<field::ColumnFamily>      CellColumnFamilyPrefix;
    typedef CellFieldPrefix<field::ColumnQualifier>   CellColumnQualifierPrefix;
    typedef CellFieldPrefix<field::Value>             CellValuePrefix;

} // namespace kdi

#endif // KDI_CELL_FIELD_H
