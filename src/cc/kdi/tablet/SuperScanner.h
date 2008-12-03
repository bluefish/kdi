//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-08-11
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

#ifndef KDI_TABLET_SUPERSCANNER_H
#define KDI_TABLET_SUPERSCANNER_H

#include <kdi/tablet/forward.h>
#include <kdi/cell.h>
#include <kdi/scan_predicate.h>
#include <warp/interval.h>
#include <boost/thread/mutex.hpp>
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
    SuperTabletCPtr superTablet;
    ScanPredicate pred;

    warp::IntervalSet<std::string> remainingRows;
    warp::Interval<std::string> currentRows;

    CellStreamPtr scanner;
    Cell lastCell;

    boost::mutex mutex;

public:
    SuperScanner(SuperTabletCPtr const & superTablet,
                 ScanPredicate const & pred);

    bool get(Cell & x);
    void reopen();

private:
    /// Clip remainingRows
    void setMinRow(warp::IntervalPoint<std::string> const & minRow);

    /// Remove currentRows from remainingRows and try openScanner()
    bool openNextScanner();

    /// Try to open scanner on the remainingRows
    bool openScanner();
};


#endif // KDI_TABLET_SUPERSCANNER_H
