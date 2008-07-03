//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-11-20
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

#ifndef KDI_MESSAGE_CHANNEL_H
#define KDI_MESSAGE_CHANNEL_H

#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/system/error_code.hpp>

#include "error.h"
#include <tr1/unordered_map>
#include <vector>

// Channel
#include <stdint.h>
#include "warp/strref.h"

// MessageChecksum
#include "warp/util.h"

namespace kdi {
namespace message {

    struct MessageHeader
    {
        uint32_t length;
        uint32_t channel;
        uint32_t checksum;
    };

    struct Message
    {
        MessageHeader header;
        std::vector<char> body;
    };

    class Channel;
    class Connection;
    class Server;

    using warp::strref_t;

    typedef boost::shared_ptr<Connection> ConnectionPtr;

    class Checksum;

} // namespace message
} // namespace kdi

#define EX_DEFINE_STD_EXCEPTION(NAME, BASE, WHAT)               \
    class NAME : public BASE                                    \
    {                                                           \
    public:                                                     \
        char const * what() const throw () { return WHAT; }     \
    }


//----------------------------------------------------------------------------
// Checksum
//----------------------------------------------------------------------------
class kdi::message::Checksum
{
    enum { INITIAL_SUM = 0xdeadbeef };

    uint32_t sum;
     
    void update(void const * data, size_t len)
    {
        char const * begin = static_cast<char const *>(data);
        char const * end = begin + len;
        for(; begin != end; ++begin)
            sum ^= (sum << 7) + (sum >> 3) + uint32_t(*begin);
    }

    void update(uint32_t x)
    {
        update(&x, sizeof(x));
    }

public:
    explicit Checksum(Message const & m) :
        sum(INITIAL_SUM)
    {
        update(m.header.length);
        update(m.header.channel);
        if(!m.body.empty())
            update(&m.body[0], m.body.size());
    }

    Checksum(uint32_t length, uint32_t channel, void const * body) :
        sum(INITIAL_SUM)
    {
        update(length);
        update(channel);
        update(body, length);
    }

    operator uint32_t() const { return sum; }
};


//----------------------------------------------------------------------------
// Channel
//----------------------------------------------------------------------------
class kdi::message::Channel
    : private boost::noncopyable
{
    friend class kdi::message::Connection;
    enum { CHANNEL_ANY = ~uint32_t(0) };

    ConnectionPtr conn;
    uint32_t channelNumber;

public:
    /// Open the next available channel number
    Channel(ConnectionPtr const & conn);

    /// Open a specific channel number
    Channel(ConnectionPtr const & conn, uint32_t number);

    /// Close channel and clean up
    virtual ~Channel();

    /// Get the channel number associated with the channel.
    uint32_t getChannelNumber() const { return channelNumber; }

    /// Send a message over the channel
    void send(strref_t msg);

    /// Callback issued when a message is received on the channel.
    virtual void onChannelReceived(strref_t msg, ErrorPtr const & err) = 0;

    /// Callback issued when a message is sent over the channel.
    virtual void onChannelSent(ErrorPtr const & err) = 0;
};


//----------------------------------------------------------------------------
// Connection
//----------------------------------------------------------------------------
class kdi::message::Connection
    : private boost::noncopyable
{
public:
    EX_DEFINE_STD_EXCEPTION(connection_error, std::exception,
                            "connection error");

    EX_DEFINE_STD_EXCEPTION(channel_error, std::exception,
                            "channel error");
    
    EX_DEFINE_STD_EXCEPTION(short_header, connection_error,
                            "short message header");

    EX_DEFINE_STD_EXCEPTION(short_body, connection_error,
                            "short message body");

    EX_DEFINE_STD_EXCEPTION(length_overflow, connection_error,
                            "message length exceeds maximum");

    EX_DEFINE_STD_EXCEPTION(bad_checksum, connection_error,
                            "incorrect message checksum");

    EX_DEFINE_STD_EXCEPTION(bad_type, connection_error,
                            "unknown message type");

    EX_DEFINE_STD_EXCEPTION(channel_in_use, channel_error,
                            "channel number already in use");

    EX_DEFINE_STD_EXCEPTION(channel_closed, channel_error,
                            "channel is closed");

    enum { MAX_MESSAGE_LENGTH = 1 << 20 };

private:
    typedef boost::asio::io_service io_service;
    typedef boost::asio::ip::tcp tcp;
    typedef boost::system::error_code error_code;

    typedef std::tr1::unordered_map<uint32_t, Channel *> channel_map_t;

    tcp::socket sock;
    channel_map_t channelMap;
    uint32_t nextChannel;

    // Incoming data
    Message incoming;

private:
    struct ReadError;
    struct WriteError;

    /// Report error to all handlers and close down connection
    template <class ErrorReporter, class Error>
    void fatalError(ErrorReporter const & report, Error const & err)
    {
        using std::vector;

        std::cerr << "fatal error: " << err.what() << std::endl;

        // Bundle the error into an error pointer
        ErrorPtr errPtr = wrapError(err);

        // Get a copy of the channel handler list
        vector<Channel *> handlers;
        handlers.reserve(channelMap.size());
        for(channel_map_t::const_iterator i = channelMap.begin();
            i != channelMap.end(); ++i)
        {
            handlers.push_back(i->second);
        }
        
        // Report error to each handler
        for(vector<Channel *>::const_iterator i = handlers.begin();
            i != handlers.end(); ++i)
        {
            report(*i, errPtr);
        }

        // Close socket, which should cancel pending async operations
        sock.close();
    }

    void handle_header_read(error_code const & err, size_t bytesRead);
    void handle_body_read(error_code const & err, size_t bytesRead);
public:
    void readIncoming();

public:
    /// Initialize for an incoming connection
    Connection(io_service & io);

    /// Establish a connection to a remote server
    Connection(io_service & io, tcp::endpoint const & remoteAddr);

    tcp::socket & getSocket() { return sock; };

private:
    friend class kdi::message::Channel;

    /// Pick an unused channel number
    uint32_t pickUnusedChannel();

    /// Open a new channel using the number it advertises.  If the
    /// channel number is Channel::CHANNEL_ANY, assign an unused
    /// channel number.
    void openChannel(Channel * channel);

    void closeChannel(Channel * handler);

    void send(strref_t msg, Channel * handler);
};


#endif // KDI_MESSAGE_CHANNEL_H
