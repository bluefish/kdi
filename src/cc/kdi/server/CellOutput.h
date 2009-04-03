//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-03
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

#ifndef KDI_SERVER_CELLOUTPUT_H
#define KDI_SERVER_CELLOUTPUT_H

#include <kdi/strref.h>

namespace kdi {
namespace server {

    class CellOutput;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// CellOutput
//----------------------------------------------------------------------------
class kdi::server::CellOutput
{
public:
    /// Emit a cell to the output stream
    virtual void emitCell(strref_t row, strref_t column, int64_t timestamp,
                          strref_t value) = 0;

    /// Emit an erasure cell to the output stream
    virtual void emitErasure(strref_t row, strref_t column,
                             int64_t timestamp) = 0;

    /// Get the number of cells (and erasures) in the output
    virtual size_t getCellCount() const = 0;

    /// Get the approximate data size of the output in bytes
    virtual size_t getDataSize() const = 0;

protected:
    ~CellOutput() {}
};

#endif // KDI_SERVER_CELLOUTPUT_H
