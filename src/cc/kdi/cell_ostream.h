//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-27
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

#ifndef KDI_CELL_OSTREAM_H
#define KDI_CELL_OSTREAM_H

#include <kdi/cell.h>
#include <warp/repr.h>
#include <warp/timestamp.h>
#include <iostream>

namespace kdi {

    /// Adapter for printing a Cell as fast as possible.
    struct WithFastOutput
    {
        Cell const & cell;
        explicit WithFastOutput(Cell const & cell) : cell(cell) {}
    };

    inline std::ostream & operator<<(std::ostream & o, Cell const & cell)
    {
        using warp::ReprEscape;
        using warp::Timestamp;

        if(!cell)
            return o << "(NULL)";

        int64_t ts = cell.getTimestamp();

        o << "(\""
          << ReprEscape(cell.getRow())
          << "\",\""
          << ReprEscape(cell.getColumn())
          << "\",";
        if(-10000 < ts && ts < 10000)
            o << '@' << ts;
        else
            o << Timestamp::fromMicroseconds(ts);
        o << ",\"";
        if(cell.isErasure())
            o << "ERASED";
        else
            o << ReprEscape(cell.getValue());
        return o << "\")";
    }

    inline std::ostream & operator<<(std::ostream & o, WithFastOutput const & cell)
    {
        using warp::ReprEscape;
        Cell const & c = cell.cell;
        return o << "(\""
                 << ReprEscape(c.getRow())
                 << "\",\""
                 << ReprEscape(c.getColumn())
                 << "\",@"
                 << c.getTimestamp()
                 << ",\""
                 << ReprEscape(c.getValue())
                 << "\")";
    }

} // namespace kdi

#endif // KDI_CELL_OSTREAM_H
