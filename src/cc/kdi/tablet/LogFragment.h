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

#ifndef KDI_TABLET_LOGFRAGMENT_H
#define KDI_TABLET_LOGFRAGMENT_H

#include <kdi/tablet/Fragment.h>
#include <boost/shared_ptr.hpp>

namespace kdi {
namespace tablet {

    /// Tablet fragment for a mutable table backed by a shared log.
    class LogFragment;

    typedef boost::shared_ptr<LogFragment> LogFragmentPtr;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// LogFragment
//----------------------------------------------------------------------------
class kdi::tablet::LogFragment
    : public kdi::tablet::Fragment
{
    TablePtr logTable;
    std::string uri;

public:
    explicit LogFragment(std::string const & uri);
    TablePtr const & getWritableTable() { return logTable; }

    // Fragment API
    virtual CellStreamPtr scan(ScanPredicate const & pred) const;

    virtual bool isImmutable() const;
    virtual std::string getFragmentUri() const;
    virtual std::string getDiskUri() const;
    virtual size_t getDiskSize(warp::Interval<std::string> const & rows) const;

    virtual flux::Stream< std::pair<std::string, size_t> >::handle_t
    scanIndex(warp::Interval<std::string> const & rows) const;
};


#endif // KDI_TABLET_LOGFRAGMENT_H
