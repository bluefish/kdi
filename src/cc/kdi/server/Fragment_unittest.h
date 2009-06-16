//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-06-12
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

#ifndef KDI_SERVER_FRAGMENT_UNITTEST_H
#define KDI_SERVER_FRAGMENT_UNITTEST_H

#include <kdi/server/Fragment.h>
#include <kdi/server/CellOutput.h>
#include <kdi/strref.h>
#include <sstream>
#include <stdio.h>

namespace kdi {
namespace server {

    void genTestCells(size_t nCells, strref_t column, CellOutput & output)
    {
        char row[32];
        for(size_t i = 0; i < nCells; ++i)
        {
            snprintf(row, sizeof(row), "%020lu", i);
            output.emitCell(row, column, 0, "");
        }
    }


} // namespace server
} // namespace kdi

#endif // KDI_SERVER_FRAGMENT_UNITTEST_H
