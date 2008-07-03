//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-11-07
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

#ifndef KDI_CLIENT_CONNECTION_MANAGER_H
#define KDI_CLIENT_CONNECTION_MANAGER_H

#include "client_connection.h"
#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/future/future.hpp>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <map>

namespace kdi {
namespace client {

    /// Establishes connections to remote servers and coordinates
    /// connection reuse.
    class ConnectionManager;

} // namespace client
} // namespace kdi

//----------------------------------------------------------------------------
// ConnectionManager
//----------------------------------------------------------------------------
class kdi::client::ConnectionManager
    : private boost::noncopyable
{
    /// State for a connect() request.  This may initiate multiple
    /// connection Attempts, but at most one will be returned as a
    /// ClientConnection.
    class Request;

    /// Shared pointer to connection Request.
    typedef boost::shared_ptr<Request> RequestPtr;

    /// State for a connection in progress.  There is one Attempt per
    /// destination at any given time.  If the attempt succeeds, it
    /// may become a full ClientConnection.
    class Attempt;

    /// Shared pointer to connection Attempt.
    typedef boost::shared_ptr<Attempt> AttemptPtr;

    /// Weak pointer to connection Attempt.
    typedef boost::weak_ptr<Attempt> AttemptWeakPtr;

private:
    typedef boost::asio::ip::tcp::endpoint endpoint_t;
    typedef std::map<endpoint_t, ClientConnectionWeakPtr> established_t;
    typedef std::map<endpoint_t, AttemptWeakPtr> pending_t;

    boost::asio::io_service io;
    boost::asio::io_service::work work;
    boost::scoped_ptr<boost::thread> worker;
    boost::asio::ip::tcp::resolver resolver;

    // These are only accessed by the worker thread while the worker
    // is running.
    established_t established;
    pending_t pending;

private:
    ConnectionManager();
    ~ConnectionManager();

    /// Entry point for worker thread.
    void workerMain();

    /// Called by Request in worker thread when a connection has
    /// successfully established.
    void setEstablished(ClientConnectionPtr const & connection);

    /// Called by Attempt in worker thread when a new connection
    /// attempt is started.
    void beginPending(AttemptPtr const & attempt);

    /// Called by Attempt in worker thread when a the connection
    /// attempt is completed.
    void endPending(AttemptPtr const & attempt);

    /// Callback for async host name resolution.
    void handle_resolve(boost::promise<ClientConnectionPtr> promise,
                        boost::system::error_code const & err,
                        boost::asio::ip::tcp::resolver::iterator addrs);

public:
    /// Get the singleton instance of the ConnectionManager.
    static ConnectionManager & get();

    /// Make a connection to a server at the remote host and port.  If
    /// the address resolves to multiple endpoints, they will be tried
    /// in turn until a connection succeeds.
    /// @return A future ClientConnectionPtr
    boost::future<ClientConnectionPtr> connect(strref_t host, strref_t port);
};


#endif // KDI_CLIENT_CONNECTION_MANAGER_H
