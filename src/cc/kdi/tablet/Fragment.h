//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-08-05
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

#ifndef KDI_TABLET_FRAGMENT_H
#define KDI_TABLET_FRAGMENT_H

#include <kdi/table.h>
#include <warp/interval.h>
#include <flux/stream.h>
#include <string>
#include <utility>

namespace kdi {
namespace tablet {

    /// A piece of a Tablet.
    class Fragment;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// Fragment
//----------------------------------------------------------------------------
class kdi::tablet::Fragment
{
public:
    virtual ~Fragment() {}

    // Table read API

    virtual CellStreamPtr scan(ScanPredicate const & pred) const = 0;

    // Fragment API

    /// Indicates if the Fragment is immutable.
    virtual bool isImmutable() const = 0;

    /// Get the URI that can be used to load this Fragment from the
    /// ConfigManager.
    virtual std::string getFragmentUri() const = 0;

    /// Get the URI of the file backing this Fragment.
    virtual std::string getDiskUri() const = 0;

    /// Get the approximate disk space used by the cells in the given
    /// row range.
    virtual size_t getDiskSize(warp::Interval<std::string> const & rows) const = 0;

    /// Get the approximate disk space used by the cells in the given
    /// row interval set.  The default implementation simply calls the
    /// row range version for each interval in the set and may lead to
    /// overcounting for fragmented sets.
    virtual size_t getDiskSize(warp::IntervalSet<std::string> const & rows) const;

    /// Get the approximate total disk size of this fragment.
    virtual size_t getDiskSize() const;

    /// Scan the index of the fragment within the given row range,
    /// returning (row, incremental-size) pairs.  The incremental size
    /// approximates the on-disk size of the cells between the last
    /// returned row key (or the given lower bound) and the current
    /// row key.
    virtual flux::Stream< std::pair<std::string, size_t> >::handle_t
    scanIndex(warp::Interval<std::string> const & rows) const = 0;
};


#endif // KDI_TABLET_FRAGMENT_H
