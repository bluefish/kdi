//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-27
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

#ifndef KDI_CELL_FILTER_H
#define KDI_CELL_FILTER_H

#include <kdi/scan_predicate.h>
#include <kdi/cell.h>

namespace kdi {

    /// Wrap a CellStream input with filters appropriate to implement
    /// the given ScanPredicate.  If the scan is unconstrained, the
    /// input stream is returned as-is.
    CellStreamPtr applyPredicateFilter(ScanPredicate const & pred,
                                       CellStreamPtr const & input);

    /// Make a CellStream filter that implements the given row
    /// inclusion predicate.
    CellStreamPtr makeRowFilter(
        ScanPredicate::StringSetCPtr const & keepSet);

    /// Make a CellStream filter that implements the given column
    /// inclusion predicate.
    CellStreamPtr makeColumnFilter(
        ScanPredicate::StringSetCPtr const & keepSet);

    /// Make a CellStream filter that implements the given timestamp
    /// inclusion predicate.
    CellStreamPtr makeTimestampFilter(
        ScanPredicate::TimestampSetCPtr const & keepSet);

    /// Make a CellStream filter that implements the given max history
    /// constraint.  The \c maxHistory parameter is interpreted as
    /// specified by ScanPredicate.
    CellStreamPtr makeHistoryFilter(int maxHistory);

    /// Make a CellStream filter that strips out Cell erasures.
    CellStreamPtr makeErasureFilter();

} // namespace kdi

#endif // KDI_CELL_FILTER_H
