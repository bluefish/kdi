//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/attic/message/server_test.cc#1 $
//
// Created 2007/11/21
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "channel.h"
#include <boost/scoped_ptr.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <iostream>
#include <boost/format.hpp>

using namespace kdi;
using namespace kdi::message;
using namespace std;
using boost::format;

namespace asio = boost::asio;
using asio::ip::tcp;
using boost::system::error_code;


//----------------------------------------------------------------------------
// EchoChannel
//----------------------------------------------------------------------------
class EchoChannel : public kdi::message::Channel
{
public:
    EchoChannel(ConnectionPtr const & conn) :
        Channel(conn, 0)
    {
        cerr << "Opening EchoChannel " << this << endl;
    }

    virtual ~EchoChannel()
    {
        cerr << "Closing EchoChannel " << this << endl;
    }

    virtual void onChannelReceived(strref_t msg, ErrorPtr const & err)
    {
        if(err)
            cerr << "Receive error: " << err->what() << endl;
        else
            send(msg);
    }

    virtual void onChannelSent(ErrorPtr const & err)
    {
        if(err)
            cerr << "Send error: " << err->what() << endl;
    }
};


//----------------------------------------------------------------------------
// Accept handlers
//----------------------------------------------------------------------------
void handleAccept(tcp::acceptor & acceptor, ConnectionPtr const & ptr,
                  error_code const & err);

void startAccept(tcp::acceptor & acceptor)
{
    // Make a new connection object
    ConnectionPtr p(new Connection(acceptor.io_service()));

    // Wait for a new connection
    acceptor.async_accept(
        p->getSocket(),
        boost::bind(
            &handleAccept,
            boost::ref(acceptor),
            p,
            asio::placeholders::error)
        );
}

void handleAccept(tcp::acceptor & acceptor, ConnectionPtr const & ptr,
                  error_code const & err)
{
    if(err)
    {
        cerr << format("Error accepting on %s: %s")
            % acceptor.local_endpoint() % err << endl;
        return;
    }

    // Got a connection, make it start reading
    ptr->readIncoming();
  
    // Allocate a channel (count on it to delete itself)
    new EchoChannel(ptr);

    // Wait for a new connection
    startAccept(acceptor);
}

//----------------------------------------------------------------------------
// main
//----------------------------------------------------------------------------
int main(int ac, char ** av)
{
    // Set up server
    asio::io_service io;
    // tcp::acceptor acceptor(io);
    // acceptor.open(tcp::v4());
    // acceptor.set_option(tcp::acceptor::reuse_address(true));
    // acceptor.listen();
    tcp::acceptor acceptor(io, tcp::endpoint(tcp::v4(), 43210));

    // Report
    cerr << "Listening at: " << acceptor.local_endpoint() << endl;

    // Wait for a new connection
    startAccept(acceptor);

    // Go
    io.run();
    return 0;
}
