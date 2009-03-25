//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-22
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

#ifndef KDI_MUX_MULTITABLE_H
#define KDI_MUX_MULTITABLE_H

#include <kdi/table.h>
#include <warp/assocvec.h>
#include <warp/interval.h>
#include <warp/string_range.h>
#include <boost/noncopyable.hpp>
#include <vector>
#include <string>

namespace kdi {
namespace mux {

    class MultiTable;

} // namespace mux
} // namespace kdi

namespace warp { class Config; }

//----------------------------------------------------------------------------
// MultiTable
//----------------------------------------------------------------------------
class kdi::mux::MultiTable
    : public kdi::Table,
      private boost::noncopyable
{
public:
    explicit MultiTable(warp::Config const & cfg);

    void set(strref_t r, strref_t c, int64_t t, strref_t v);
    void erase(strref_t r, strref_t c, int64_t t);
    void sync();

    CellStreamPtr scan() const;
    CellStreamPtr scan(ScanPredicate const & pred) const;

    RowIntervalStreamPtr scanIntervals() const;

private:
    struct Group
    {
        warp::IntervalSet<std::string> columns;
        std::string uri;
        mutable TablePtr table;
        bool dirty;

        Group() : dirty(false) {}

        /// If there is any overlap between the predicate columns and
        /// the columns in this group, return a scan clipped to this
        /// group.  Otherwise return null.
        CellStreamPtr maybeScan(ScanPredicate const & pred) const;

    private:
        CellStreamPtr openScan(ScanPredicate const & pred) const;
    };
    
    typedef std::vector<Group> group_vec;
    typedef warp::AssocVector<std::string, Group *, warp::less, warp::AVDirectStorage<std::string, Group *> > column_map;

private:
    inline Group * findColumnGroup(strref_t col) const;
    inline Group * openColumnGroup(strref_t col);

    void loadConfig(warp::Config const & cfg);

private:
    group_vec groups;
    column_map index;
    Group * defaultGroup;
    Group * intervalGroup;
};

#endif // KDI_MUX_MULTITABLE_H
