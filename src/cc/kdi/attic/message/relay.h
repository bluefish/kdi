//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-11-19
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

#ifndef KDI_MESSAGE_RELAY_H
#define KDI_MESSAGE_RELAY_H

#include <boost/asio.hpp>

namespace kdi {
namespace message {

    class Relay;
    class RelayClient;
    class RelayServer;

} // namespace message
} // namespace kdi

//----------------------------------------------------------------------------
// Connection
//----------------------------------------------------------------------------
class Connection
{
    typedef boost::asio::ip::tcp tcp;
    typedef boost::asio::io_service io_service;
    typedef warp::strref_t strref_t;
    typedef boost::system::error_code error_code;

    typedef boost::function<void (error_code const &)> error_handler;
    typedef boost::function<void (error_code const &, strref_t)> message_handler;
    typedef boost::function<void (error_code const &, strref_t)> message_handler;

    typedef error_handler const & connect_handler;
    typedef error_handler const & send_handler;
    typedef message_handler const & receive_handler;

    tcp::socket sock;

public:
    explicit Connection(io_service & io);
    ~Connection();

    void connect(strref_t host, strref_t port);
    void connect(strref_t host, strref_t port, connect_handler handler);

    void connect(tcp::endpoint const & dst);
    void connect(tcp::endpoint const & dst, connect_handler handler);

    void listen();
    void listen(strref_t port);

private:
    friend class kdi::message::Channel;

    void send(uint32_t channel, strref_t msg, send_handler const & handler);
    void receive(uint32_t channel, receive_handler const & handler);
};

//----------------------------------------------------------------------------
// Client
//----------------------------------------------------------------------------
class kdi::message::Client : public kdi::message::Connection
{
public:
    /// Establish a Client to the given host/port
    Client(io_service & io, strref_t host, strref_t port);

    /// Establish a Client to the given destination
    Client(io_service & io, tcp::endpoint const & dst);

    /// Asynchronously establish a Client to the given host/port
    Client(io_service & io, strref_t host, strref_t port,
          connect_handler const & handler);

    /// Asynchronously establish a Client to the given destination
    Client(io_service & io, tcp::endpoint const & dst,
          connect_handler const & handler);

    ~Client();
};


#endif // KDI_MESSAGE_RELAY_H
