//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-11-01
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

#if 0

#include "async_table.h"

using namespace kdi;

//----------------------------------------------------------------------------
// FixedQueue
//----------------------------------------------------------------------------
template <class T, size_t N>
class FixedQueue
{
    enum { DATA_SZ = sizeof(T) * N };
    char data[DATA_SZ];
    T * out;
    T * in;
    size_t sz;

    static void advance(T * & p)
    {
        if(++p == reinterpret_cast<T *>(&data[DATA_SZ]))
            p = reinterpret_cast<T *>(data);
    }

public:
    FixedQueue() :
        out(reinterpret_cast<T *>(data)),
        in(reinterpret_cast<T *>(data)),
        sz(0)
    {
    }
    
    ~FixedQueue()
    {
        while(sz)
            pop();
    }
    
    void pop()
    {
        out->~T();
        advance(out);
        --sz;
    }

    void push(T const & x)
    {
        new (in) T(x);
        advance(in);
        ++sz;
    }

    T const & front() const { return *out; }
    bool empty() const { return sz == 0; }
    bool full() const { return sz == N; }
    size_t size() const { return sz; }
};


//----------------------------------------------------------------------------
// ScanBuffer
//----------------------------------------------------------------------------
class ScanBuffer
{
    deque<Cell> cells;

public:
    bool eof;

public:
    explicit ScanBuffer(size_t maxSz) :
        cells(maxSz), eof(false)
    {
    }

    void put(Cell const & x)
    {
        cells.push_back(x);
    }

    void get(Cell & x)
    {
        x = cells.front();
        cells.pop_front();
    }

    bool empty() const { return cells.empty(); }
    bool full() const { return cells.size() == cells.capacity(); }
};

typedef boost::shared_ptr<ScanBuffer> ScanBufferPtr;


//----------------------------------------------------------------------------
// AsyncCellStream
//----------------------------------------------------------------------------
class AsyncCellStream
{
public:
    enum Status { OK, WOULD_BLOCK, END_OF_STREAM };

private:
    // Worker members
    CellStreamPtr scan;

    // Client members
    WorkerPoolPtr workerPool;
    ScanBufferPtr currentBuffer;

    // Shared members
    SyncQueue<ScanBufferPtr> readyQueue;


    // Worker functions
    void scanMore(ScanBufferPtr const & dst)
    {
        // Scan to fill buffer until either: A) run out of buffer
        // space, or B) hit the end of the scan stream.  The
        // termination condition is indicated by the EOF flag.
        Cell x;
        for(;;)
        {
            if(dst->full())
            {
                // The buffer is full, clear the EOF flag.
                dst->eof = false;
                break;
            }
            if(!scan->get(x))
            {
                // We're at the end of the scan, set the EOF flag.
                dst->eof = true;
                break;
            }

            // Got an item and there's still room.  Store it in the
            // buffer and loop.
            dst->put(x);
        }

        // We're done filling the buffer.  Put it in the queue of
        // ready buffers for the client.
        readyQueue.push(dst);
    }


    // Client functions
    Status fetchBuffer()
    {
        for(;;)
        {
            // If our current buffer has the EOF flag set, it is the
            // end of the stream.
            if(currentBuffer->eof):
                return END_OF_STREAM;
            
            // Get the next buffer from the ready queue without
            // blocking.
            ScanBufferPtr nextBuffer;
            if(!readyQueue.get(nextBuffer, false))
                return WOULD_BLOCK;

            // Swap the next buffer into the current buffer's place.
            currentBuffer.swap(nextBuffer);

            // If we haven't seen the end of stream, submit the empty
            // buffer for more scanning.
            if(!currentBuffer->eof)
                submitScan(nextBuffer);

            // If the buffer contains something, return OK.
            // Otherwise, loop again.  It is probably an empty buffer
            // with the EOF flag set.
            if(!currentBuffer->empty())
                return OK;
        }
    }

    void submitScan(ScanBufferPtr const & dst)
    {
        workerPool.submit(
            boost::bind(
                &AsyncCellStream::scanMore,
                shared_from_this(),
                dst
                )
            );
    }

public:
    AsyncCellStream(...) :
        currentBuffer(new ScanBuffer(BUFFER_SZ))
    {
        // Immediately submit a scan buffer.  The first call to get()
        // will find the empty current buffer and try to swap it out.
        ScanBufferPtr buf(new ScanBuffer(BUFFER_SZ));
        submitScan(buf);
    }

    /// Get the next Cell in the scan.
    /// @return \c OK if a Cell was available, \c END_OF_STREAM if the
    /// scan has been exhausted, or \c WOULD_BLOCK if there's nothing
    /// available for reading yet.
    Status get(Cell & x)
    {
        // If the current buffer is empty, try to fetch a new one out
        // of the ready queue.
        if(currentBuffer->empty())
        {
            Status s = fetchBuffer();
            if(s != OK)
                return s;
        }

        // We now have a buffer with something in it.
        currentBuffer->get(x);
        return OK;
    }

    void cancel()
    {
        // 
    }
};

#endif
