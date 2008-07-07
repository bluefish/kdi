//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-11-05
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

#ifndef KDI_CLIENT_CLIENT_TABLE_H
#define KDI_CLIENT_CLIENT_TABLE_H

#include <kdi/table.h>
#include <kdi/marshal/cell_block_builder.h>

namespace kdi {
namespace client {

    class ClientTable;

} // namespace client
} // namespace kdi

//----------------------------------------------------------------------------
// ClientTable
//----------------------------------------------------------------------------
class kdi::client::ClientTable : public kdi::Table
{
    typedef boost::mutex mutex_t;
    typedef boost::condition condition_t;
    typedef mutex_t::scoped_lock lock_t;

    ClientConnectionPtr conn;
    int tableId;

    warp::Builder builder;
    CellBlockBuilder cellBuilder;
    size_t bufferSz;

    condition_t syncComplete;
    bool syncPending;

    mutex_t mutex;

    ErrorBuffer errors;

private:
    // Table completion interface
    friend class ClientConnection;

    void onSyncComplete()
    {
        // note that sync has completed
        syncPending = false;

        // signal completion condition
        syncComplete.notify_one();
    }

    void onError(std::string const & message)
    {
        // collect errors in a buffer and throw them in a later call
        // to the table interface

        lock_t l(mutex);
        errors.append(message);
    }


private:
    void throwErrors()
    {
        lock_t l(mutex);
        errors.maybeThrow();
    }

    void flushMutations()
    {
        assert(cellBuilder.getCellCount() > 0);

        // throw if we have errors
        throwErrors();

        // Build mutation list
        builder.finalize();
        CellBlockBufferPtr buf(new CellBlockBuffer(builder.getFinalSize()));
        builder.exportTo(buf->getRaw());

        // Pass mutations to connection
        conn->applyMutations(tableId, buf);

        // Reset buffer
        builder.reset();
        cellBuilder.reset();
    }
    
public:
    explicit ClientTable(std::string const & uri) :
        cellBuilder(&builder),
        bufferSz(64 << 10),
        syncPending(false)
    {
        // Parse URI -- need a host and port
        Uri u(uri);
        UriAuthority ua(u.authority);
        if(!ua.host || !ua.port)
            raise<RuntimeError>("missing host and/or port: %s", uri);

        // Make connection
        conn = ConnectionManager::get().connect(ua.host, ua.port);

        // Open table
        tableId = conn->openTable(uri, this);
    }

    ~ClientTable()
    {
        // Flush any pending mutations
        if(cellBuilder.getCellCount() > 0)
            flushMutations();

        // Close table
        conn->closeTable(tableId);
    }


    // Table interface
    virtual void set(strref_t row, strref_t column, int64_t timestamp,
                     strref_t value)
    {
        // buffer mutations
        cellBuilder.appendCell(row, column, timestamp, value);

        // if buffer is full, flush mutations
        if(cellBuilder.getDataSize() >= bufferSz)
            flushMutations();
    }

    virtual void erase(strref_t row, strref_t column, int64_t timestamp)
    {
        // buffer mutations
        cellBuilder.appendErasure(row, column, timestamp);

        // if buffer is full, flush mutations
        if(cellBuilder.getDataSize() >= bufferSz)
            flushMutations();
    }

    virtual CellStreamPtr scan() const
    {
        // create a ClientScanner
        CellStreamPtr p(new ClientScanner(conn, tableId));
        return p;
    }

    virtual CellStreamPtr scan(ScanPredicate const & pred) const
    {
        // create a ClientScanner
        CellStreamPtr p(new ClientScanner(conn, tableId, pred));
        return p;
    }

    virtual void sync()
    {
        // Flush any pending mutations
        if(cellBuilder.getCellCount() > 0)
            flushMutations();

        // note that we have a sync pending
        syncPending = true;

        // send a sync across the connection
        conn->syncTable(tableId);

        for(;;)
        {
            // throw if we have errors
            throwErrors();

            // if sync is complete, break
            if(!syncPending)
                break;

            // wait on completion condition
            lock_t l(mutex);
            syncComplete.wait(l);
        }
    }
};


#endif // KDI_CLIENT_CLIENT_TABLE_H
