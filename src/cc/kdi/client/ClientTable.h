//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-06
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

#ifndef KDI_CLIENT_CLIENTTABLE_H
#define KDI_CLIENT_CLIENTTABLE_H

#include <kdi/table.h>
#include <kdi/rpc/TabletServer.h>
#include <kdi/rpc/PackedCellWriter.h>
#include <boost/scoped_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <string>

namespace kdi {
namespace client {

    class ClientTable;

    // Forward declaration
    class Sorter;

} // namespace client
} // namespace kdi

//----------------------------------------------------------------------------
// ClientTable
//----------------------------------------------------------------------------
class kdi::client::ClientTable
    : public Table,
      private boost::noncopyable
{
public:
    ClientTable(strref_t host, strref_t port, strref_t tableName,
                bool syncOnFlush);
    ~ClientTable();

    void set(strref_t row, strref_t col, int64_t ts, strref_t val);
    void erase(strref_t row, strref_t col, int64_t ts);
    void sync();

    CellStreamPtr scan() const;
    CellStreamPtr scan(ScanPredicate const & pred) const;

private:
    void checkKey(strref_t row, strref_t col, int64_t ts);
    void flush(bool waitForSync);

private:
    std::string tableName;
    int64_t lastCommit;
    bool syncOnFlush;
    bool needSync;
    bool needSort;

    boost::scoped_ptr<Sorter> sorter;
    kdi::rpc::TabletServerPrx server;
    kdi::rpc::PackedCellWriter out;
};


#endif // KDI_CLIENT_CLIENTTABLE_H
