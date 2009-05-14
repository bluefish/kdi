//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-13
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

#ifndef KDI_SERVER_ROWCOLLECTOR_H
#define KDI_SERVER_ROWCOLLECTOR_H

#include <kdi/server/CellOutput.h>
#include <vector>
#include <string>

namespace kdi {
namespace server {

    class RowCollector;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// RowCollector
//----------------------------------------------------------------------------
class kdi::server::RowCollector
    : public kdi::server::CellOutput
{
public:
    RowCollector(std::vector<std::string> & rows,
                 CellOutput & output) :
        rows(rows),
        output(output)
    {
    }

public:                         // CellOutput API
    void emitCell(strref_t row, strref_t column, int64_t timestamp,
                  strref_t value)
    {
        addRow(row);
        output.emitCell(row, column, timestamp, value);
    }

    void emitErasure(strref_t row, strref_t column,
                     int64_t timestamp)
    {
        addRow(row);
        output.emitErasure(row, column, timestamp);
    }

    size_t getCellCount() const
    {
        return output.getCellCount();
    }

    size_t getDataSize() const
    {
        return output.getDataSize();
    }

private:
    void addRow(strref_t row)
    {
        if(rows.empty() || rows.back() != row)
            rows.push_back(row.toString());
    }

private:
    std::vector<std::string> & rows;
    CellOutput & output;
};

#endif // KDI_SERVER_ROWCOLLECTOR_H
