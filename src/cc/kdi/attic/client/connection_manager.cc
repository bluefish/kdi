//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-11-13
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

#include "connection_manager.h"
#include <boost/system/system_error.hpp>

using namespace kdi;
using namespace kdi::client;
using boost::system::error_code;
using boost::system::system_error;
using boost::asio::ip::tcp;
namespace asio = boost::asio;

//----------------------------------------------------------------------------
// ConnectionManager::Request
//----------------------------------------------------------------------------
class ConnectionManager::Request
    : private boost::noncopyable
{
    ConnectionManager * mgr;
    boost::promise<ClientConnectionPtr> promise;
    size_t nOutstanding;
    bool success;

public:
    Request(ConnectionManager * mgr,
            boost::promise<ClientConnectionPtr> promise,
            size_t nOutstanding) :
        mgr(mgr),
        promise(promise),
        nOutstanding(nOutstanding),
        success(false)
    {
        EX_CHECK_NULL(mgr);
    }

    void onAttemptSuccess(ClientConnectionPtr const & connection)
    {
        // Decrement requests outstanding
        --nOutstanding;

        // Only do this if we haven't already had a connection
        // success
        if(!success)
        {
            success = true;

            // Set the active connection
            mgr->setEstablished(connection);

            // Fulfill the promise
            promise.set(connection);

            // Would be nice to deref other attempts here, resulting
            // in cancellations
        }
    }

    void onAttemptFailure(error_code const & error)
    {
        // Decrement requests outstanding
        --nOutstanding;

        // If that was the last attempt and we haven't already
        // succeeded, we lose
        if(!nOutstanding && !success)
        {
            // Notify promise of failure
            promise.set_exception(system_error(error));
        }
    }
};


//----------------------------------------------------------------------------
// ConnectionManager::Attempt
//----------------------------------------------------------------------------
class ConnectionManager::Attempt
    : public boost::enable_shared_from_this<Attempt>,
      private boost::noncopyable
{
    typedef boost::shared_ptr<tcp::socket> socket_ptr;

    socket_ptr sock;
    asio::deadline_timer timeout;
    tcp::endpoint dst;

    ConnectionManager * mgr;
    vector<RequestPtr> requests;

    void endAttempt()
    {
        // Clear out the request list so we can't send conflicting
        // notifications if cancellation events come in later
        requests.clear();

        // Remove ourself from the pending connection list, then lose
        // that pointer so we don't try to do it twice
        if(mgr)
        {
            mgr->endPending(shared_from_this());
            mgr = 0;
        }
    }

    void notifyFailure(error_code const & err)
    {
        assert(err);

        // Tell each request about the failure
        for(vector<RequestPtr>::const_iterator i = requests.begin();
            i != requests.end(); ++i)
        {
            (*i)->onAttemptFailure(err);
        }

        // This attempt is done
        endAttempt();
    }

    void notifySuccess(ClientConnectionPtr const & connection)
    {
        assert(connection);

        // Tell each request about the success
        for(vector<RequestPtr>::const_iterator i = requests.begin();
            i != requests.end(); ++i)
        {
            (*i)->onAttemptSuccess(connection);
        }

        // This attempt is done
        endAttempt();
    }

    void handle_connect(error_code const & err)
    {
        if(err)
            return notifyFailure(err);

        // Normally, start a handshake here.  But for now, just
        // assume that a successful connection means we win.

        // Cancel timeout timer
        timeout.cancel();

        // Make a new connection and tell all the requests of the
        // success
        ClientConnectionPtr connection(new ClientConnection(sock));
        notifySuccess(connection);
    }

    void handle_timeout()
    {
        // Notify requests of timeout
        notifyFailure(boost::system::posix_error::timed_out);

        // Close socket to cancel pending operations
        sock->close();
    }

public:
    Attempt(asio::io_service & io, ConnectionManager * mgr) :
        sock(new tcp::socket(io)), timeout(io), mgr(mgr)
    {
        EX_CHECK_NULL(mgr);
    }

    void connect(tcp::endpoint const & dst)
    {
        // Add ourselves to pending list
        mgr->beginPending(shared_from_this());

        // Start a connection
        sock->async_connect(
            dst,
            boost::bind(
                &Attempt::handle_connect,
                shared_from_this(),
                asio::placeholders::error)
            );

        // Start a timeout timer
        timer.expired_from_now(boost::posix_time::seconds(5));
        timer.async_wait(
            boost::bind(
                &Attempt::handle_timeout,
                shared_from_this())
            );
    }

    void addRequest(RequestPtr const & req)
    {
        requests.push_back(req);
    }

    tcp::endpoint const & getRemoteEndpoint() const
    {
        return dst;
    }
};


//----------------------------------------------------------------------------
// ConnectionManager
//----------------------------------------------------------------------------
ConnectionManager::ConnectionManager() :
    io(), work(io), resolver(io)
{
    // Start worker thread
    worker.reset(
        new boost::thread(
            boost::bind(
                &ConnectionManager::workerMain,
                this)
            )
        );
}

ConnectionManager::~ConnectionManager()
{
    // Stop io_service
    io.stop();

    // Wait for worker to join
    worker->join();
}

void ConnectionManager::workerMain()
{
    try {
        // Run the io_service loop
        io.run();
    }
    catch(...) {
        // Terminate on uncaught exceptions
        std::terminate();
    }
}

void ConnectionManager::setEstablished(ClientConnectionPtr const & connection)
{
    established[connection->getRemoteEndpoint()] = connection;
}

void ConnectionManager::beginPending(AttemptPtr const & attempt)
{
    pending[attempt->getRemoteEndpoint()] = attempt;
}

void ConnectionManager::endPending(AttemptPtr const & attempt)
{
    pending.erase(attempt->getRemoteEndpoint());
}

void ConnectionManager::handle_resolve(boost::promise<ClientConnectionPtr> promise,
                                       error_code const & err,
                                       tcp::resolver::iterator addrs)
{
    // Check for error
    if(err)
    {
        err.set_exception(system_error(err));
        return;
    }

    // Default iterator is the end
    tcp::resolver::iterator addrEnd;

    // Make sure the host resolved to at least one address
    if(addrs == addrEnd)
    {
        err.set_exception(host_not_found());
        return;
    }

    // Check to see if we already have a connection established
    size_t nAddrs = 0;
    for(tcp::resolver::iterator i = addrs; i != addrEnd; ++i, ++nAddrs)
    {
        endpoint_t dst = i->endpoint();
        established_t::const_iterator ii = established.find(dst);
        if(ii != established.end())
        {
            if(ClientConnectionPtr conn = ii->second.lock())
            {
                // Found an established connection
                promise.set(conn);
                return;
            }
        }
    }

    // There is no established connection.  Create a new
    // connection request and start (or piggyback) attempts for
    // each possible destination.
    RequestPtr req(new Request(this, promise, nAddrs));
    for(tcp::resolver::iterator i = addrs; i != addrEnd; ++i)
    {
        endpoint_t dst = i->endpoint();

        AttemptWeakPtr & weakAttemptRef = pending[dst];
        AttemptPtr attempt = weakAttemptRef.lock();

        if(!attempt)
        {
            // Start a new connection attempt for the target
            // endpoint
            attempt.reset(new Attempt(io, this));
            attempt->connect(dst);
        }

        // Add request to the attempt's completion list
        attempt->addRequest(req);
    }
}

ConnectionManager::ConnectionManager & get()
{
    static ConnectionManager theMgr;
    return theMgr;
}

boost::future<ClientConnectionPtr>
ConnectionManager::connect(strref_t host, strref_t port)
{
    // Make our ClientConnectionPtr promise
    boost::promise<ClientConnectionPtr> p;

    // Start the connection chain by doing host resolution
    resolver.async_resolve(
        tcp::resolver::query(str(host), str(port)),
        boost::bind(
            &ConnectionManager::handle_resolve,
            this,
            p,
            asio::placeholders::error,
            asio::placeholders::iterator)
        );

    // Return a future on our promise
    return p;
}
