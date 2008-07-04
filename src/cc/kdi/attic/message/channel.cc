//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-03
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

#include <kdi/attic/message/channel.h>
#include <boost/bind.hpp>
#include <boost/array.hpp>

#include <iostream>
#include <boost/format.hpp>

using boost::format;
using namespace std;

using namespace kdi;
using namespace kdi::message;

namespace asio = boost::asio;

//----------------------------------------------------------------------------
// Channel
//----------------------------------------------------------------------------
Channel::Channel(ConnectionPtr const & conn) :
    conn(conn), channelNumber(CHANNEL_ANY)
{
    EX_CHECK_NULL(conn);
    conn->openChannel(this);
}

Channel::Channel(ConnectionPtr const & conn, uint32_t number) :
    conn(conn), channelNumber(number)
{ 
    EX_CHECK_NULL(conn);
    conn->openChannel(this);
}

Channel::~Channel()
{
    conn->closeChannel(this);
}

void Channel::send(strref_t msg)
{
    conn->send(msg, this);
}

//----------------------------------------------------------------------------
// Connection details
//----------------------------------------------------------------------------
struct Connection::ReadError
{
    void operator()(Channel * handler, ErrorPtr const & err) const
    {
        handler->onChannelReceived(strref_t(), err);
    }
};

struct Connection::WriteError
{
    void operator()(Channel * handler, ErrorPtr const & err) const
    {
        handler->onChannelSent(err);
    }
};

//----------------------------------------------------------------------------
// Connection
//----------------------------------------------------------------------------
void Connection::handle_header_read(error_code const & err, size_t bytesRead)
{
    cerr << format("header read: %d bytes") % bytesRead << endl;
    if(err)
        cerr << "error: " << err << endl;

    if(err)
        return fatalError(ReadError(), boost::system::system_error(err));

    if(bytesRead != sizeof(MessageHeader))
        return fatalError(ReadError(), short_header());

    if(incoming.header.length > MAX_MESSAGE_LENGTH)
        return fatalError(ReadError(), length_overflow());

    incoming.body.resize(incoming.header.length);
    if(!incoming.body.empty())
    {
        asio::async_read(
            sock,
            asio::buffer(incoming.body),
            boost::bind(
                &Connection::handle_body_read,
                this,
                asio::placeholders::error,
                asio::placeholders::bytes_transferred
                )
            );
    }
    else
    {
        handle_body_read(err, 0);
    }
}

void Connection::handle_body_read(error_code const & err, size_t bytesRead)
{
    cerr << format("body read: %d bytes") % bytesRead << endl;
    if(err)
        cerr << "error: " << err << endl;

    if(err)
        return fatalError(ReadError(), boost::system::system_error(err));

    if(bytesRead != incoming.body.size())
        return fatalError(ReadError(), short_body());

    if(Checksum(incoming) != incoming.header.checksum)
        return fatalError(ReadError(), bad_checksum());

    // call receive handler for channel
    channel_map_t::const_iterator i = channelMap.find(incoming.header.channel);
    if(i != channelMap.end())
    {
        char const * p = incoming.body.empty() ? 0 : &incoming.body[0];
        i->second->onChannelReceived(
            warp::binary_data(p, incoming.body.size()),
            ErrorPtr());
    }
    else
        cerr << "received message for closed channel: "
             << incoming.header.channel << endl;

    // Read next message
    readIncoming();
}

void Connection::readIncoming()
{
    asio::async_read(
        sock,
        asio::buffer(&incoming.header, sizeof(incoming.header)),
        boost::bind(
            &Connection::handle_header_read,
            this,
            asio::placeholders::error,
            asio::placeholders::bytes_transferred)
        );
}

Connection::Connection(io_service & io) :
    sock(io), nextChannel(1)
{
}

Connection::Connection(io_service & io, tcp::endpoint const & remoteAddr) :
    sock(io), nextChannel(2)
{
    // Connect, handshake, start read
    sock.connect(remoteAddr);

    // Start read
    readIncoming();
}

uint32_t Connection::pickUnusedChannel()
{
    uint32_t num = nextChannel;
    nextChannel += 2;
    return num;
}

void Connection::openChannel(Channel * channel)
{
    // Get the channel number (get a reference because we may have
    // to modify it in a second)
    uint32_t & number = channel->channelNumber;

    // If the channel number is the special ANY channel, assign an
    // unused channel number
    if(number == Channel::CHANNEL_ANY)
        number = pickUnusedChannel();
        
    // Get a reference to the channel object in the map.  If the
    // channel doesn't exist, this will create the entry and
    // initialize it to null.  If it already exists, we'll get the
    // reference to the previously assigned channel.
    Channel * & c = channelMap[number];

    // Don't allow conflicts on channel numbers
    if(c)
        throw channel_in_use();

    // The map entry was null.  Assign it now.
    c = channel;
}

void Connection::closeChannel(Channel * handler)
{
    // Get the channel number (get a reference because we'll have
    // to modify it in a second)
    uint32_t & number = handler->channelNumber;

    // Remove the channel from the channel map
    channelMap.erase(number);

    // Reset the channel number
    number = Channel::CHANNEL_ANY;
}

void Connection::send(strref_t msg, Channel * handler)
{
    // Get channel number
    uint32_t number = handler->getChannelNumber();

    // Only send messages on open channels
    if(number == Channel::CHANNEL_ANY)
        throw channel_closed();

    // Serialize message into output buffer

    // If there is no write pending, kick off a new one


    // Do a sync write for now
    MessageHeader hdr;
    hdr.length = msg.size();
    hdr.channel = number;
    hdr.checksum = Checksum(hdr.length, hdr.channel, msg.begin());

    boost::array<asio::const_buffer, 2> parts = {{
        asio::buffer(&hdr, sizeof(MessageHeader)),
        asio::buffer(msg.begin(), msg.size())
    }};

    cerr << format("writing message (len=%d)") % hdr.length << endl;
    asio::write(sock, parts);
    cerr << "done writing" << endl;
}
