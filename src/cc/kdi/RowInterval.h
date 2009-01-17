//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-10-28
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

#ifndef KDI_ROWINTERVAL_H
#define KDI_ROWINTERVAL_H

#include <warp/interval.h>
#include <string>
#include <iostream>

namespace kdi {

    /// An interval range of rows
    class RowInterval
        : public warp::Interval<std::string>
    {
        typedef warp::Interval<std::string> super;

    public:
        RowInterval() : super() {}
        explicit RowInterval(super const & rows) : super(rows) {}
        explicit RowInterval(point_t const & lower, point_t const & upper) :
            super(lower, upper) {}

        /// Return the interval as a row predicate string
        std::string toRowPredicate() const;
    };

    /// Serialize a RowInterval as a row predicate string
    std::ostream & operator<<(std::ostream & out, RowInterval const & x);

} // namespace kdi

#endif // KDI_ROWINTERVAL_H
