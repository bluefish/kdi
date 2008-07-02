//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/flux/serialize.h#1 $
//
// Created 2006/04/03
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef FLUX_SERIALIZE_H
#define FLUX_SERIALIZE_H

#include "stream.h"
#include "warp/file.h"
#include <assert.h>

namespace flux
{
    //------------------------------------------------------------------------
    // WriteFixed
    //------------------------------------------------------------------------
    struct WriteFixed
    {
        template <class T>
        void operator()(warp::FilePtr const & fp, T const & x)
        {
            assert(fp);
            fp->write(&x, sizeof(T), 1);
        }
    };


    //------------------------------------------------------------------------
    // Serialize
    //------------------------------------------------------------------------
    template <class T, class Writer = WriteFixed>
    class Serialize : public Stream<T>
    {
    public:
        typedef Serialize<T, Writer> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

    private:
        warp::FilePtr fp;
        Writer write;

    public:
        explicit Serialize(warp::FilePtr const & fp,
                           Writer const & write = Writer()) :
            fp(fp), write(write)
        {
            assert(fp);
        }

        void put(T const & x)
        {
            write(fp, x);
        }

        void flush()
        {
            fp->flush();
        }
    };


    template <class T>
    typename Serialize<T>::handle_t makeSerialize(warp::FilePtr const & fp)
    {
        typename Serialize<T>::handle_t h(new Serialize<T>(fp));
        return h;
    }

    template <class T, class W>
    typename Serialize<T,W>::handle_t makeSerialize(warp::FilePtr const & fp,
                                                    W const & write)
    {
        typename Serialize<T,W>::handle_t h(new Serialize<T,W>(fp,write));
        return h;
    }
}

#endif // FLUX_SERIALIZE_H
