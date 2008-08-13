//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/SuperScanner.h $
//
// Created 2008/08/11
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_TABLET_SUPERSCANNER_H
#define KDI_TABLET_SUPERSCANNER_H

#include <kdi/tablet/forward.h>
#include <kdi/cell.h>
#include <kdi/scan_predicate.h>
#include <warp/interval.h>
#include <string>

namespace kdi {
namespace tablet {

    class SuperScanner;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// SuperScanner
//----------------------------------------------------------------------------
class kdi::tablet::SuperScanner
    : public kdi::CellStream
{
    SuperTabletPtr superTablet;
    ScanPredicate pred;

    warp::IntervalSet<std::string> remainingRows;
    warp::Interval<std::string> currentRows;

    CellStreamPtr scanner;
    Cell lastCell;

public:
    SuperScanner(SuperTabletPtr const & superTablet,
                 ScanPredicate const & pred);

    bool get(Cell & x);
    void reopen();

private:
    void setMinRow(warp::IntervalPoint<std::string> const & minRow);
    bool openNextScanner();
};


#endif // KDI_TABLET_SUPERSCANNER_H
