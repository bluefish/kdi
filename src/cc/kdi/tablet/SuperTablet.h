//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-07-14
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

#ifndef KDI_TABLET_SUPERTABLET_H
#define KDI_TABLET_SUPERTABLET_H

#include <kdi/table.h>
#include <kdi/tablet/forward.h>
#include <kdi/tablet/MetaConfigManager.h>
#include <vector>

namespace kdi {
namespace tablet {

    /// A collection of Tablets in the same table.
    class SuperTablet;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// SuperTablet
//----------------------------------------------------------------------------
class kdi::tablet::SuperTablet
    : public kdi::Table
{
    std::vector<TabletPtr> tablets;


    /// Get the Tablet containing the given row
    TabletPtr const & getTablet(strref_t row) const;

public:
    SuperTablet(std::string const & name,
                MetaConfigManagerPtr const & configMgr,
                SharedLoggerPtr const & logger,
                SharedCompactorPtr const & compactor);
    ~SuperTablet();

    // Table API
    virtual void set(strref_t row, strref_t column, int64_t timestamp,
                     strref_t value);
    virtual void erase(strref_t row, strref_t column, int64_t timestamp);
    virtual void insert(Cell const & x);
    virtual CellStreamPtr scan() const;
    virtual CellStreamPtr scan(ScanPredicate const & pred) const;
    virtual void sync();
};


#endif // KDI_TABLET_SUPERTABLET_H
