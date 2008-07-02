//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/attic/client/client_connection.cc#1 $
//
// Created 2007/11/13
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "client_connection.h"

using namespace kdi;
using namespace kdi::client;

class MessageHeader
{
    uint32_t length;
    uint32_t checksum;
    uint32_t type;
};

#define MESSAGE_TYPE(a,b,c,d) enum { TYPECODE = WARP_PACK4((a),(b),(c),(d)) }

//----------------------------------------------------------------------------
// Requests
//----------------------------------------------------------------------------
struct OpenRequest
{
    MESSAGE_TYPE('O','p','n','?');

    uint32_t requestId;
    warp::StringOffset uri;
};

struct CloseRequest
{
    MESSAGE_TYPE('C','l','s','?');

    uint32_t requestId;
    uint32_t serverTableId;
};

struct ApplyRequest
{
    MESSAGE_TYPE('A','p','l','?');

    uint32_t requestId;
    uint32_t serverTableId;
    warp::Offset<CellBlock> cells;
};

struct SyncRequest
{
    MESSAGE_TYPE('S','y','n','?');
    
    uint32_t requestId;
    uint32_t serverTableId;
};

struct BeginScanRequest
{
    MESSAGE_TYPE('B','S','c','?');

    enum
    {
        REQ_PT_INFINITE_LOWER_BOUND  = 0,
        REQ_PT_EXCLUSIVE_UPPER_BOUND = 1,
        REQ_PT_INCLUSIVE_LOWER_BOUND = 2,
        REQ_PT_POINT                 = 3,
        REQ_PT_INCLUSIVE_UPPER_BOUND = 4,
        REQ_PT_EXCLUSIVE_LOWER_BOUND = 5,
        REQ_PT_INFINITE_UPPER_BOUND  = 6,
    };

    warp::PointType requestToPointType(uint8_t p)
    {
        switch(p)
        {
            case REQ_PT_INFINITE_LOWER_BOUND  : return warp::PT_INFINITE_LOWER_BOUND  ;
            case REQ_PT_EXCLUSIVE_UPPER_BOUND : return warp::PT_EXCLUSIVE_UPPER_BOUND ;
            case REQ_PT_INCLUSIVE_LOWER_BOUND : return warp::PT_INCLUSIVE_LOWER_BOUND ;
            case REQ_PT_POINT                 : return warp::PT_POINT                 ;
            case REQ_PT_INCLUSIVE_UPPER_BOUND : return warp::PT_INCLUSIVE_UPPER_BOUND ;
            case REQ_PT_EXCLUSIVE_LOWER_BOUND : return warp::PT_EXCLUSIVE_LOWER_BOUND ;
            case REQ_PT_INFINITE_UPPER_BOUND  : return warp::PT_INFINITE_UPPER_BOUND  ;
        }
        raise<ValueError>("unexpected request value: %s", (uint32_t)p);
    }

    uint8_t pointTypeToRequest(warp::PointType p)
    {
        switch(p)
        {
            case warp::PT_INFINITE_LOWER_BOUND  : return REQ_PT_INFINITE_LOWER_BOUND  ;
            case warp::PT_EXCLUSIVE_UPPER_BOUND : return REQ_PT_EXCLUSIVE_UPPER_BOUND ;
            case warp::PT_INCLUSIVE_LOWER_BOUND : return REQ_PT_INCLUSIVE_LOWER_BOUND ;
            case warp::PT_POINT                 : return REQ_PT_POINT                 ;
            case warp::PT_INCLUSIVE_UPPER_BOUND : return REQ_PT_INCLUSIVE_UPPER_BOUND ;
            case warp::PT_EXCLUSIVE_LOWER_BOUND : return REQ_PT_EXCLUSIVE_LOWER_BOUND ;
            case warp::PT_INFINITE_UPPER_BOUND  : return REQ_PT_INFINITE_UPPER_BOUND  ;
        }
        raise<ValueError>("unexpected point type: %s", (uint32_t)p);
    }

    struct StringPoint
    {
        warp::StringOffset point;
        uint8_t type;
        uint8_t __pad[3];
    };

    struct TimePoint
    {
        uint64_t point;
        uint8_t type;
        uint8_t __pad[7];
    };

    struct Predicate
    {
        Offset< ArrayOffset<StringPoint> > rows;
        Offset< ArrayOffset<StringPoint> > columns;
        Offset< ArrayOffset<TimePoint>   > timestamps;
        uint64_t                           maxTimesPerCell;
    };

    uint32_t requestId;
    uint32_t serverTableId;
    warp::Offset<Predicate> predicate;
};

struct ContinueScanRequest
{
    MESSAGE_TYPE('C','S','c','?');

    uint32_t requestId;
    uint32_t serverScanId;
};

struct EndScanRequest
{
    MESSAGE_TYPE('E','S','c','?');

    uint32_t requestId;
    uint32_t serverScanId;
};

//----------------------------------------------------------------------------
// Responses
//----------------------------------------------------------------------------
struct OpenResponse
{
    MESSAGE_TYPE('O','p','n','!');
    
    uint32_t requestId;
    uint32_t serverTableId;
};

struct ScanResponse
{
    MESSAGE_TYPE('S','c','n','!');

    uint32_t requestId;
    uint32_t serverScanId;
    warp::Offset<CellBlock> cells;
    uint8_t isFinal;
};

struct SyncCompleteResponse
{
    MESSAGE_TYPE('S','y','n','!');
    
    uint32_t requestId;
};

struct ErrorResponse
{
    MESSAGE_TYPE('E','r','r','!');

    uint32_t requestId;
    warp::StringOffset message;
};


//----------------------------------------------------------------------------
// 
//----------------------------------------------------------------------------

struct Message
{
    MessageHeader header;
    vector<char> body;
};


#define DECLARE_EXCEPTION(NAME, BASE, WHAT)             \
    class NAME : public BASE                            \
    {                                                   \
    public:                                             \
        char const * what() const { return WHAT; }      \
    }


class MessageChannel
{
    DECLARE_EXCEPTION(short_header, std::runtime_error,
                      "short message header");

    DECLARE_EXCEPTION(short_body, std::runtime_error,
                      "short message body");

    DECLARE_EXCEPTION(length_overflow, std::runtime_error,
                      "message length exceeds maximum");

    DECLARE_EXCEPTION(bad_checksum, std::runtime_error,
                      "incorrect message checksum");

    DECLARE_EXCEPTION(bad_type, std::runtime_error,
                      "unknown message type");

    
    enum
    {
        MAX_MESSAGE_LENGTH = 1 << 20
    };



    class MessageError : private boost::noncopyable
    {
    public:
        virtual ~ErrorBase() {}
        virtual void rethrow() const = 0;
    };

    template <class T>
    class TypedMessageError : public MessageError
    {
        T ex;
    public:
        explicit TypedMessageError(T const & ex) : ex(ex) {}
        virtual void rethrow() const { throw ex; }
    };

    template <class T>
    void setError(T const & ex)
    {
        // Set only once so we don't change the pointer while another
        // thread is trying to rethrow it
        if(!channelError)
            channelError.reset(new TypedMessageError<T>(ex));
    }

    void rethrowError()
    {
        if(channelError)
            channelError->rethrow();
    }


    typedef boost::shared_ptr<boost::asio::ip::tcp::socket> socket_ptr;
    
    socket_ptr sock;

    // Pointer to a rethrowable exception.  This is only set once, and
    // only by the async IO thread.  It is only thrown from the main
    // interface thread(s).
    boost::scoped_ptr<MessageError> channelError;

    // Data used by async thread
    
    boost::shared_ptr<Message> incoming;

private:
    void handle_header_read(error_code const & err, size_t bytesRead)
    {
        if(err)
        {
            setError(boost::system::system_error(err));
            sock->close();
            return;
        }

        if(bytesRead != sizeof(MessageHeader))
        {
            setError(short_header());
            sock->close();
            return;
        }

        if(incoming->header.length > MAX_MESSAGE_LENGTH)
        {
            setError(length_overflow());
            sock->close();
            return;
        }

        if(incoming->header.length > 0)
        {
            incoming->body.resize(incoming->header.length);
            asio::async_read(
                sock,
                asio::buffer(incoming->body),
                boost::bind(
                    &MessageChannel::handle_body_read,
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

    void handle_body_read(error_code const & err, size_t bytesRead)
    {
        if(err)
        {
            setError(boost::system::system_error(err));
            sock->close();
            return;
        }

        if(bytesRead != incoming->body.size())
        {
            setError(short_body());
            sock->close();
            return;
        }

        if(!incoming->verifyChecksum())
        {
            setError(bad_checksum());
            sock->close();
            return;
        }

        switch(incoming->header.type)
        {
            default:
                setError(bad_type());
                sock->close();
                return;

            case OpenResponse::TYPECODE:
            {
                
                OpenResponse const * r = incoming->cast<OpenResponse>();
                break;
            }

            case ScanResponse::TYPECODE:
                break;
            case SyncCompleteResponse::TYPECODE:
                break;

            case ErrorResponse::TYPECODE:
                break;
        }

        readIncoming();
    }


    void readIncoming()
    {
        incoming.reset(new Message);

        asio::async_read(
            sock,
            asio::buffer(&incoming->header, sizeof(MessageHeader)),
            boost::bind(
                &MessageChannel::handle_header_read,
                this,
                asio::placeholders::error,
                asio::placeholders::bytes_transferred)
            );
    }

public:
    MessageChannel(socket_ptr const & sock) :
        sock(sock)
    {
        EX_CHECK_NULL(sock);

        readIncoming();
    }



    
};

//----------------------------------------------------------------------------
// ClientConnection
//----------------------------------------------------------------------------
ClientConnection::ClientConnection(socket_ptr const & sock) :
    sock(sock)
{
    EX_CHECK_NULL(sock);
}

int ClientConnection::openTable(std::string const & tableUri, ClientTable * handler)
{
    // Assign tableDescriptor

    // Initialize client state for tableDescriptor
    //   state.serverTableId = <uninit>
    //   state.connected = false
    //   state.handler = handler
    //   state.error = ()

    // Send open request
}

void ClientConnection::closeTable(int tableDescriptor)
{
    // Verify that tableDescriptor is valid

    // Send close request

    // Clean up client state for tableDescriptor
}

void ClientConnection::applyMutations(int tableDescriptor, CellBlockBufferCPtr const & buf)
{
    // Verify that tableDescriptor is valid

    // Send mutation request
}

void ClientConnection::syncTable(int tableDescriptor)
{
    // Verify that tableDescriptor is valid

    // Send mutation request
}

int ClientConnection::beginScan(int tableDescriptor, ClientScanner * handler)
{
    // Verify that tableDescriptor is valid

    // Assign scanDescriptor

    // Initialize client state for scanDescriptor
    //   state.serverScanId = <uninit>
    //   state.connected = false
    //   state.handler = handler
    //   state.error = ()

    // Send begin scan request
}

int ClientConnection::beginScan(int tableDescriptor, ScanPredicate const & pred,
                                ClientScanner * handler)
{
    // Verify that tableDescriptor is valid

    // Assign scanDescriptor

    // Initialize client state for scanDescriptor
    //   state.serverScanId = <uninit>
    //   state.connected = false
    //   state.handler = handler
    //   state.error = ()

    // Send begin scan request with predicate
}

void ClientConnection::continueScan(int scanDescriptor)
{
    // Verify that scanDescriptor is valid

    // Send continue scan request
}

void ClientConnection::endScan(int scanDescriptor)
{
    // Verify that scanDescriptor is valid

    // Send end scan request

    // Clean up client state for scanDescriptor
}
