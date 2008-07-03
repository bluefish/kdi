//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-11-05
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

#ifndef KDI_CLIENT_CLIENT_CONNECTION_H
#define KDI_CLIENT_CLIENT_CONNECTION_H

#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>

namespace kdi {
namespace client {

    /// Connection from this client process to a remote server process
    /// over a socket.  Table operations are channeled through this
    /// channel, and multiple tables and scans may be proxied
    /// simultaneously through the same connection.
    class ClientConnection;

    /// Shared pointer to ClientConnection.
    typedef boost::shared_ptr<ClientConnection> ClientConnectionPtr;

    /// Weak pointer to ClientConnection.
    typedef boost::weak_ptr<ClientConnection> ClientConnectionWeakPtr;

} // namespace client
} // namespace kdi

//----------------------------------------------------------------------------
// ClientConnection
//----------------------------------------------------------------------------
class kdi::client::ClientConnection
    : public boost::enable_shared_from_this<kdi::client::ClientConnection>,
      private boost::noncopyable
{
    typedef kdi::marshal::CellBlockBufferPtr CellBlockBufferPtr;
    typedef kdi::marshal::CellBlockBufferCPtr CellBlockBufferCPtr;

    typedef boost::asio::ip::tcp tcp;
    typedef boost::shared_ptr<tcp::socket> socket_ptr;

    socket_ptr sock;

public:
    /// Create a ClientConnection over an established socket.
    explicit ClientConnection(socket_ptr const & sock);

private:
    friend class ClientTable;

    /// Open a Table over the connection, allocating client and server
    /// side resources for the table.  The passed ClientTable is used
    /// to handle callbacks for network events.  This call must be
    /// balanced with a call to closeTable().  This call will block
    /// until the table has been opened.
    /// @return Table descriptor to use when referencing this table.
    int openTable(std::string const & tableUri, ClientTable * handler);

    /// Release client and server side resources associated with the
    /// table descriptor.  The client side resources are released
    /// immediately.  A close message is dispatched to the server, but
    /// the function returns without awaiting acknowledgement.
    void closeTable(int tableDescriptor);

    /// Apply the mutation buffer to the given table.  The function
    /// call returns immediately and the message is sent
    /// asynchronously, so the buffer contents should never be
    /// modified after passing them into this function.
    void applyMutations(int tableDescriptor, CellBlockBufferCPtr const & buf);

    /// Issue a sync call on the given table.  This call returns
    /// immediately.  The request is dispatched and serviced
    /// asynchronously.
    void syncTable(int tableDescriptor);
    
private:
    friend class ClientScanner;

    /// Initiate a full scan of the given table, allocating client and
    /// server side resources for the table.  The passed ClientScanner
    /// is used to handle callbacks for network events.  This call
    /// must be balanced with a call to endScan().  This call will
    /// block until the scan has been initiated.  The first batch of
    /// cells for the scan will be sent asynchronously.
    /// @return Scan descriptor to use when referencing this scan.
    int beginScan(int tableDescriptor, ClientScanner * handler);

    /// Initiate a predicate scan of the given table, allocating
    /// client and server side resources for the table.  The passed
    /// ClientScanner is used to handle callbacks for network events.
    /// This call must be balanced with a call to endScan().  This
    /// call will block until the scan has been initiated.  The first
    /// batch of cells for the scan will be sent asynchronously.
    /// @return Scan descriptor to use when referencing this scan.
    int beginScan(int tableDescriptor, ScanPredicate const & pred,
                  ClientScanner * handler);

    /// Tell server to send another batch of cells from the named
    /// scan.  This call returns immediately.  The request will be
    /// dispatched and serviced asynchronously.
    void continueScan(int scanDescriptor);

    /// Release client and server side resources associated with the
    /// scan descriptor.  The client side resources are released
    /// immediately.  A close message is dispatched to the server, but
    /// the function returns without awaiting acknowledgement.
    void endScan(int scanDescriptor);

public:
};



#endif // KDI_CLIENT_CLIENT_CONNECTION_H
