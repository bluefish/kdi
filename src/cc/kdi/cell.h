//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-23
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

#ifndef KDI_CELL_H
#define KDI_CELL_H

#include <kdi/strref.h>
#include <kdi/sdstore_cell.h>
#include <flux/stream.h>
#include <boost/shared_ptr.hpp>

namespace kdi {

    using sdstore::Cell;

    typedef flux::Stream<Cell> CellStream;
    typedef boost::shared_ptr<CellStream> CellStreamPtr;

    using sdstore::makeCell;
    using sdstore::makeCellErasure;

} // namespace kdi

#endif // KDI_CELL_H
