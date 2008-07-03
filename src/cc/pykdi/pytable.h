//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-01-11
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

#ifndef PYKDI_PYTABLE_H
#define PYKDI_PYTABLE_H

#include <kdi/table.h>
#include <string>

namespace pykdi {

    class PyTable;

    // Forward declaration
    class PyScan;

} // namespace pykdi


//----------------------------------------------------------------------------
// PyTable
//----------------------------------------------------------------------------
class pykdi::PyTable
{
    std::string uri;
    kdi::TablePtr table;

public:
    explicit PyTable(std::string const & uri);

    void set(std::string const & row, std::string const & col,
             int64_t rev, std::string const & val);
    void erase(std::string const & row, std::string const & col,
               int64_t rev);

    void sync();

    PyScan scan() const;
    PyScan scan(kdi::ScanPredicate const & pred) const;
    PyScan scan(std::string const & pred) const;

public:
    static void defineWrapper();
};

#endif // PYKDI_PYTABLE_H
