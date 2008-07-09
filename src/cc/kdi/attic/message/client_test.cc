//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-03
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
#include <warp/options.h>
#include <warp/uri.h>
#include <warp/string_range.h>
#include <warp/syncqueue.h>
#include <ex/exception.h>

#include <boost/asio.hpp>
#include <boost/format.hpp>
#include <boost/variant.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>

#include <iostream>
#include <string>

using namespace kdi;
using namespace kdi::message;
using namespace warp;
using namespace ex;
using namespace std;
using boost::format;

namespace asio = boost::asio;
using asio::ip::tcp;

//----------------------------------------------------------------------------
// TestChannel
//----------------------------------------------------------------------------
class TestChannel : public kdi::message::Channel
{
    typedef boost::variant<std::string, ErrorPtr> response_t;
    SyncQueue<response_t> responses;

public:
    TestChannel(ConnectionPtr const & conn) : Channel(conn, 0) {}
    
    virtual void onChannelReceived(strref_t msg, ErrorPtr const & err)
    {
        if(err)
            responses.push(err);
        else
            responses.push(str(msg));
    }

    virtual void onChannelSent(ErrorPtr const & err)
    {
        if(err)
            responses.push(err);
    }

    void report()
    {
        response_t r;
        if(!responses.pop(r))
            raise<RuntimeError>("end of responses");

        if(ErrorPtr const * p = boost::get<ErrorPtr const>(&r))
            (*p)->rethrow();

        cout << r << endl;
    }
};


//----------------------------------------------------------------------------
// getAuthorityEndpoint
//----------------------------------------------------------------------------
tcp::endpoint getAuthorityEndpoint(asio::io_service & io, strref_t uri)
{
    // Parse URI
    UriAuthority auth(Uri(uri).authority);

    // Check that we've defined a host and port
    if(!auth.host || !auth.port)
        raise<ValueError>("URI needs host and port: %s", uri);

    // Set up resolver query
    tcp::resolver r(io);
    tcp::resolver::query q(str(auth.host), str(auth.port));

    // Resolve address and return the first IPv4 address we find
    for(tcp::resolver::iterator it = r.resolve(q), end; it != end; ++it)
    {
        if(it->endpoint().protocol() == tcp::v4())
            return it->endpoint();
    }

    // Didn't find any IPv4 addresses.  :(
    raise<RuntimeError>("couldn't resolve URI to a IPv4 address: %s", uri);
}

//----------------------------------------------------------------------------
// main
//----------------------------------------------------------------------------
int main(int ac, char ** av)
{
    // Set up options
    OptionParser op("%prog [options]");
    {
        using namespace boost::program_options;
        op.addOption("server,s", value<string>(), "Server URI");
    }
    
    // Parse options
    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    // Get server
    string server;
    if(!opt.get("server", server))
        op.error("need --server");

    // Make an io_service
    asio::io_service io;

    // Connect to server
    tcp::endpoint endpoint = getAuthorityEndpoint(io, server);
    cout << format("connecting to %s (%s)")
        % server % endpoint << endl;
    ConnectionPtr conn(new Connection(io, endpoint));

    // Start the async thread
    boost::thread ioThread(
        boost::bind(
            &asio::io_service::run,
            &io)
        );

    // Open a test channel
    TestChannel chan(conn);

    // Send three messages that should be echoed back
    chan.send("Hello there");
    chan.send("How are things?");
    chan.send("Goodbye");

    // Report three received messages
    chan.report();
    chan.report();
    chan.report();
    
    // Shutdown
    io.stop();
    ioThread.join();

    // Exit
    return 0;
}
