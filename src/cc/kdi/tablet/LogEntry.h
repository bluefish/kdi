//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-20
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

#ifndef KDI_TABLET_LOGENTRY_H
#define KDI_TABLET_LOGENTRY_H

#include <kdi/marshal/cell_block.h>

namespace kdi {
namespace tablet {

    struct LogEntry;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// LogEntry
//----------------------------------------------------------------------------
struct kdi::tablet::LogEntry
    : public kdi::marshal::CellBlock
{
    enum {
        TYPECODE = WARP_PACK4('T','b','L','g'),
        VERSION = 1 + kdi::marshal::CellBlock::VERSION,
        FLAGS = 0,
        ALIGNMENT = 8,
    };

    warp::StringOffset tabletName;
};

#endif // KDI_TABLET_LOGENTRY_H
