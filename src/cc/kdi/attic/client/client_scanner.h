//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/attic/client/client_scanner.h#1 $
//
// Created 2007/11/06
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_CLIENT_CLIENT_SCANNER_H
#define KDI_CLIENT_CLIENT_SCANNER_H

#include "kdi/cell.h"
#include "client_connection.h"

namespace kdi {
namespace client {

    class ClientScanner;

} // namespace client
} // namespace kdi

//----------------------------------------------------------------------------
// ClientScanner
//----------------------------------------------------------------------------
class kdi::client::ClientScanner : public kdi::CellStream
{
    typedef kdi::marshal::CellBlockBufferPtr CellBlockBufferPtr;
    typedef std::pair<CellBlockBufferPtr, bool> scan_pair_t;

    typedef boost::mutex mutex_t;
    typedef mutex_t::scoped_lock lock_t;

    struct ScanBlock
    {
        CellBlockBufferPtr buffer;
        CellData const * next;
        CellData const * end;
        bool eof;

        
        ScanBlock() :
            next(0), end(0), eof(false)
        {
        }

        void reset(scan_pair_t const & p)
        {
            buffer = p->first;
            eof = p->second;

            if(buffer)
            {
                CellBlock const * block = buffer->get();
                next = block->cells.begin();
                end = block->cells.end();
            }
            else
            {
                next = end = 0;
            }
        }

        bool empty() const
        {
            return next == end;
        }

        void get(Cell & x)
        {
            if(next->value)
            {
                x = makeCell(next->key.row,
                             next->key.column,
                             next->key.timestamp,
                             next->value);
            }
            else
            {
                x = makeErasureCell(next->key.row,
                                    next->key.column,
                                    next->key.timestamp);
            }
            ++next;
        }
    };

    ClientConnectionPtr conn;
    int scanId;

    SyncQueue<scan_pair_t> readyQueue;

    ScanBlock current;

    ErrorBuffer errors;
    mutex_t mutex;

private:

    friend class ClientConnection;

    // this class is like AsyncScanner, except the buffer type is
    // probably different, and the background function that fills the
    // buffer is an event-triggered handler driven by network traffic

    void onReceive(CellBlockBufferPtr const & buffer, bool eof)
    {
        // Put buffer in ready queue
        readyQueue.push(scan_pair_t(buffer, eof));

        // Cancel queue waits if EOF flag is set on buffer
        if(eof)
            readyQueue.cancelWaits();
    }

    void onError(std::string const & message)
    {
        lock_t l(mutex);
        errors.append(message);

        // Cancel queue waits
        readyQueue.cancelWaits();
    }

private:

    void throwErrors()
    {
        lock_t l(mutex);
        errors.maybeThrow();
    }

    bool fetchBuffer()
    {
        // Need to throw errors from connection in here

        for(;;)
        {
            // Throw errors if we have any.
            throwErrors();

            // If our current buffer has the EOF flag set, it is the
            // end of the stream.
            if(current.eof)
                return false;
            
            // Get the next buffer from the ready queue without
            // blocking.
            scan_pair_t next;
            if(!readyQueue.get(next))
            {
                // Queue cancellation can be caused by errors -- throw
                // them if we have any.
                throwErrors();
                return false;
            }

            // Reset our current scan with the next buffer.
            current.reset(next);

            // If we haven't seen the end of stream, issue a call to
            // get more cells.
            if(!current.eof)
                conn->continueScan(scanId);

            // If the buffer contains something, return OK.
            // Otherwise, loop again.  It is probably an empty buffer
            // with the EOF flag set.
            if(!current.empty())
                return true;
        }
    }


public:

    ClientScanner(ClientConnectionPtr const & conn, int tableDescriptor) :
        conn(conn)
    {
        EX_CHECK_NULL(conn);

        // Begin scan
        scanId = conn->beginScan(tableId, this);
    }

    ClientScanner(ClientConnectionPtr const & conn, int tableDescriptor,
                  ScanPredicate const & pred) :
        conn(conn)
    {
        EX_CHECK_NULL(conn);

        // Begin scan
        scanId = conn->beginScan(tableId, pred, this);
    }
    
    ~ClientScanner()
    {
        // End scan so resources can be released
        conn->endScan(scanId);
    }
    

    virtual bool get(Cell & x)
    {
        if(current.empty() && !fetchBuffer())
            return false;

        current.get(x);
        return true;
    }
    
};


#endif // KDI_CLIENT_CLIENT_SCANNER_H
