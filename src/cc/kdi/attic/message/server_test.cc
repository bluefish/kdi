//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-11-21
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

#include <kdi/attic/message/channel.h>
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
