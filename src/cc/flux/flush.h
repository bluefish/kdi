//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/flux/flush.h#1 $
//
// Created 2006/04/03
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef FLUX_FLUSH_H
#define FLUX_FLUSH_H

#include "stream.h"
#include "warp/util.h"
#include <assert.h>

namespace flux
{
    //------------------------------------------------------------------------
    // Flush
    //------------------------------------------------------------------------
    template <class T, class Sz=warp::size_of>
    class Flush : public Stream<T>
    {
    public:
        typedef Flush<T, Sz> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

        typedef typename my_t::base_handle_t base_handle_t;

    private:
        base_handle_t output;
        size_t flushSize;
        size_t curSize;
        Sz sz;

    public:
        explicit Flush(size_t flushSize, Sz const & sz = Sz()) :
            flushSize(flushSize), curSize(0), sz(sz)
        {
        }

        void pipeTo(base_handle_t const & output)
        {
            this->output = output;
        }

        void put(T const & x)
        {
            assert(output);
            output->put(x);
            curSize += sz(x);
            if(curSize > flushSize)
                flush();
        }

        void flush()
        {
            assert(output);
            output->flush();
            curSize = 0;
        }
    };


    template <class T>
    typename Flush<T>::handle_t makeFlush(size_t flushSz)
    {
        typename Flush<T>::handle_t h(new Flush<T>(flushSz));
        return h;
    }

    template <class T, class Sz>
    typename Flush<T, Sz>::handle_t makeFlush(size_t flushSz, Sz const & sz)
    {
        typename Flush<T,Sz>::handle_t h(new Flush<T,Sz>(flushSz, sz));
        return h;
    }
}

#endif // FLUX_FLUSH_H
