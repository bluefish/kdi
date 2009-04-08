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

#include <kdi/client/ClientTable.h>
#include <kdi/client/ClientScanner.h>
#include <kdi/client/Sorter.h>
#include <kdi/rpc/PackedCellReader.h>
#include <kdi/scan_predicate.h>
#include <Ice/Ice.h>
#include <sstream>

#include <warp/log.h>
using warp::log;

using namespace kdi;
using namespace kdi::client;

namespace {

    Ice::CommunicatorPtr const & getCommunicator()
    {
        int ac = 2;
        char const * av[] = { "foo", "--Ice.MessageSizeMax=32768" };

        static Ice::CommunicatorPtr com(
            Ice::initialize(ac, const_cast<char **>(av)));
        return com;
    }

    enum { FLUSH_SZ = 128 << 10 };

}

//----------------------------------------------------------------------------
// ClientTable
//----------------------------------------------------------------------------
ClientTable::ClientTable(strref_t host, strref_t port,
                         strref_t tableName, bool syncOnFlush) :
    tableName(tableName.begin(), tableName.end()),
    lastCommit(0),
    syncOnFlush(syncOnFlush),
    needSync(false),
    needSort(false)
{
    log("ClientTable: %s, %s, %s, %s", host, port, tableName, syncOnFlush);

    std::ostringstream oss;
    oss << "TabletServer:tcp -h " << host << " -p " << port;
    server = kdi::rpc::TabletServerPrx::uncheckedCast(
        getCommunicator()->stringToProxy(oss.str()));
}

ClientTable::~ClientTable()
{
    ClientTable::sync();
}

void ClientTable::set(strref_t row, strref_t col, int64_t ts, strref_t val)
{
    checkKey(row, col, ts);
    out.append(row, col, ts, val);
    if(out.getDataSize() >= FLUSH_SZ)
        flush(syncOnFlush);
}

void ClientTable::erase(strref_t row, strref_t col, int64_t ts)
{
    checkKey(row, col, ts);
    out.appendErasure(row, col, ts);
    if(out.getDataSize() >= FLUSH_SZ)
        flush(syncOnFlush);
}

void ClientTable::sync()
{
    if(out.getCellCount())
    {
        flush(true);
    }
    else if(needSync)
    {
        int64_t syncTxn = 0;
        server->sync(lastCommit, syncTxn);
        needSync = false;
    }
}

CellStreamPtr ClientTable::scan() const
{
    return scan(ScanPredicate());
}

CellStreamPtr ClientTable::scan(ScanPredicate const & pred) const
{
    CellStreamPtr p(new ClientScanner(server, tableName, pred));
    return p;
}

void ClientTable::checkKey(strref_t row, strref_t col, int64_t ts)
{
    // If we already need a sort, don't bother with further
    // checking
    if(needSort)
        return;

    // If we haven't inserted anything, there's nothing to
    // check against
    if(!out.getCellCount())
        return;

    // If this row isn't the same as the last row...
    if(int c = warp::string_compare(row, out.getLastRow()))
    {
        // We need a sort if this row is before the last
        needSort = (c < 0);
    }
    // Else rows are equal.  If this column isn't the same as
    // the last...
    else if(int c = warp::string_compare(col, out.getLastColumn()))
    {
        // We need a sort if this column is before the last
        needSort = (c < 0);
    }
    // Else rows and columns are equal...
    else
    {
        // We need a sort if this timestamp is greater than or
        // equal to the last timestamp (they are expected in
        // decreasing order, no duplicates)
        needSort = (ts >= out.getLastTimestamp());
    }
}

void ClientTable::flush(bool waitForSync)
{
    out.finish();

    warp::StringRange packed;
    if(needSort)
    {
        if(!sorter)
            sorter.reset(new Sorter);
        packed = sorter->reorder(out);
    }
    else
    {
        packed = out.getPacked();
    }

    int64_t commitTxn = 0;
    server->apply(
        tableName,
        std::make_pair(
            reinterpret_cast<Ice::Byte const *>(packed.begin()),
            reinterpret_cast<Ice::Byte const *>(packed.end())),
        kdi::rpc::kMaxTxn, waitForSync, commitTxn);

    lastCommit = commitTxn;
    needSync = !waitForSync;
    out.reset();
}

//----------------------------------------------------------------------------
// Registration
//----------------------------------------------------------------------------
#include <warp/init.h>
#include <warp/uri.h>
#include <kdi/table_factory.h>
#include <kdi/server/name_util.h>
#include <ex/exception.h>

using namespace ex;

namespace
{
    using kdi::server::isTableChar;

    std::string getTableName(strref_t path)
    {
        std::string r;
        bool omitSlash = true;
        
        for(char const * c = path.begin(); c != path.end(); ++c)
        {
            if(!isTableChar(*c))
                raise<ValueError>("bad table name: %s", path);
            
            if(*c == '/')
            {
                if(!omitSlash)
                {
                    r += *c;
                    omitSlash = true;
                }
            }
            else
            {
                r += *c;
                omitSlash = false;
            }
        }

        if(r.empty())
            raise<ValueError>("empty table name");

        return r;
    }
    
    TablePtr myOpen(std::string const & uri)
    {
        log("open: %s", uri);

        warp::Uri u(uri);
        warp::UriAuthority ua(u.authority);

        warp::StringRange host = "localhost";
        warp::StringRange port = "11000";

        if(ua.host)
            host = ua.host;
        if(ua.port)
            port = ua.port;

        bool autoSync = (warp::uriGetParameter(uri, "autosync") != "0");

        TablePtr p(
            new ClientTable(
                host, port, getTableName(u.path), autoSync));
        return p;
    }
}

WARP_DEFINE_INIT(kdi_client_ClientTable)
{
    TableFactory::get().registerTable("moon", &myOpen);
}
