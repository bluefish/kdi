//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-09-12
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

#include <kdi/table.h>
#include <kdi/scan_predicate.h>
#include <kdi/table_factory.h>
#include <kdi/RowInterval.h>

using namespace kdi;

namespace
{
    class OneInfiniteRange : public RowIntervalStream
    {
        bool got;
    public:
        OneInfiniteRange() : got(false) {}
        bool get(RowInterval & x)
        {
            if(got)
                return false;

            x.setInfinite();
            got = true;
            return true;
        }
    };
}

//----------------------------------------------------------------------------
// Table
//----------------------------------------------------------------------------
void Table::insert(Cell const & x)
{
    if(x.isErasure())
        erase(x.getRow(), x.getColumn(), x.getTimestamp());
    else
        set(x.getRow(), x.getColumn(), x.getTimestamp(), x.getValue());
}

CellStreamPtr Table::scan() const
{
    return scan(ScanPredicate());
}

CellStreamPtr Table::scan(strref_t predExpr) const
{
    return scan(ScanPredicate(predExpr));
}

TablePtr Table::open(std::string const & uri)
{
    return TableFactory::get().create(uri);
}

RowIntervalStreamPtr Table::scanIntervals() const
{
    RowIntervalStreamPtr p(new OneInfiniteRange);
    return p;
}
