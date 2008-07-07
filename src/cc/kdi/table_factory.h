//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-09-20
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

#ifndef KDI_TABLE_FACTORY_H
#define KDI_TABLE_FACTORY_H

#include <kdi/table.h>
#include <string>
#include <map>
#include <boost/function.hpp>

namespace kdi {
    
    /// Factory for registering and invoking creators of Tables.
    class TableFactory;

} // namespace kdi

//----------------------------------------------------------------------------
// TableFactory
//----------------------------------------------------------------------------
class kdi::TableFactory
{
    typedef boost::function<TablePtr (std::string const &)> creator_t;
    typedef std::map<std::string, creator_t> map_t;

    map_t reg;

    TableFactory() {}

public:
    static TableFactory & get();

    /// Register a Table creator function for the given scheme.  The
    /// scheme may be a full scheme or the first component in a
    /// compound scheme (e.g. 'foo' in 'foo+bar://...').  The creator
    /// function should accept a full URI and return a TablePtr.  It
    /// should never return null.
    void registerTable(std::string const & scheme,
                       creator_t const & creator);

    /// Create a Table instance from a URI.  A creator for the URI
    /// scheme must have been previously registered using
    /// registerTable().
    TablePtr create(std::string const & uri) const;
};


#endif // KDI_TABLE_FACTORY_H
