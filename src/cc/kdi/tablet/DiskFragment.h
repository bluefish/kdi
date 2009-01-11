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

#ifndef KDI_TABLET_DISKFRAGMENT_H
#define KDI_TABLET_DISKFRAGMENT_H

#include <kdi/tablet/Fragment.h>
#include <kdi/local/disk_table.h>

namespace kdi {
namespace tablet {

    /// Tablet fragment for an immutable on-disk table.
    class DiskFragment;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// DiskFragment
//----------------------------------------------------------------------------
class kdi::tablet::DiskFragment
    : public kdi::tablet::Fragment
{
    std::string uri;
    kdi::local::DiskTablePtr table;

public:
    explicit DiskFragment(std::string const & uri);

    virtual CellStreamPtr scan(ScanPredicate const & pred) const;

    virtual bool isImmutable() const;
    virtual std::string getFragmentUri() const;
    virtual std::string getDiskUri() const;
    virtual size_t getDiskSize(warp::Interval<std::string> const & rows) const;

    virtual flux::Stream< std::pair<std::string, size_t> >::handle_t
    scanIndex(warp::Interval<std::string> const & rows) const;
};


#endif // KDI_TABLET_DISKFRAGMENT_H
