//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/cell_filter.h#1 $
//
// Created 2007/12/27
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
