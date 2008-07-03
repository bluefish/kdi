//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-28
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

#ifndef KDI_TABLET_LOGREADER_H
#define KDI_TABLET_LOGREADER_H

#include <kdi/cell.h>
#include <kdi/marshal/cell_block.h>
#include <oort/record.h>
#include <oort/recordstream.h>

namespace kdi {
namespace tablet {

    class LogReader;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// LogReader
//----------------------------------------------------------------------------
class kdi::tablet::LogReader : public kdi::CellStream
{
    oort::RecordStreamHandle input;
    std::string tabletName;

    oort::Record logEntry;
    marshal::CellData const * nextCell;
    marshal::CellData const * endCell;

public:
    LogReader(std::string const & logFn, std::string const & tabletName);
    bool get(Cell & x);
};


#endif // KDI_TABLET_LOGREADER_H
