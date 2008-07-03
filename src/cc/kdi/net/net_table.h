//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-12
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef KDI_NET_NET_TABLE_H
#define KDI_NET_NET_TABLE_H

#include <kdi/table.h>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <string>

namespace kdi {
namespace net {

    class NetTable;

} // namespace net
} // namespace kdi

//----------------------------------------------------------------------------
// NetTable
//----------------------------------------------------------------------------
class kdi::net::NetTable
    : public kdi::Table,
      private boost::noncopyable
{
    class Impl;
    class Scanner;

    boost::shared_ptr<Impl> impl;

public:
    NetTable(std::string const & uri);
    virtual ~NetTable();

    // Table interface
    virtual void set(strref_t row, strref_t column, int64_t timestamp,
                     strref_t value);
    virtual void erase(strref_t row, strref_t column, int64_t timestamp);
    virtual CellStreamPtr scan() const;
    virtual CellStreamPtr scan(ScanPredicate const & pred) const;
    virtual void sync();
};


#endif // KDI_NET_NET_TABLE_H
