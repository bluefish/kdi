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

#include <kdi/RowInterval.h>
#include <warp/strutil.h>
#include <sstream>

std::string kdi::RowInterval::toRowPredicate() const
{
    std::ostringstream oss;
    oss << *this;
    return oss.str();
}

std::ostream & kdi::operator<<(std::ostream & out, RowInterval const & x)
{
    using namespace warp;

    IntervalPoint<std::string> const & lo = x.getLowerBound();
    IntervalPoint<std::string> const & hi = x.getUpperBound();

    if(lo.isFinite())
    {
        if(hi.isFinite())
        {
            // We have finite bounds: LOWER <? row <? UPPER
            out << reprString(lo.getValue());
            if(lo.isInclusive()) out << " <= ";
            else                 out << " < ";
            if(hi.isInclusive()) out << "row <= ";
            else                 out << "row < ";
            out << reprString(hi.getValue());
        }
        else
        {
            // Half finite bound: row ?> LOWER
            if(lo.isInclusive()) out << "row >= ";
            else                 out << "row > ";
            out << reprString(lo.getValue());
        }
    }
    else if(hi.isFinite())
    {
        // Half finite bound: row <? UPPER
        if(hi.isInclusive()) out << "row <= ";
        else                 out << "row < ";
        out << reprString(hi.getValue());
    }
    else
    {
        // Infinite bound: empty predicate
        out << "row >= \"\"";
    }
    
    return out;
}

