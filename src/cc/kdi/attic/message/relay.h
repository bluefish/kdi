//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/attic/message/relay.h#1 $
//
// Created 2007/11/19
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
