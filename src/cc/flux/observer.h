//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/flux/observer.h#1 $
//
// Created 2006/03/03
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef FLUX_OBSERVER_H
#define FLUX_OBSERVER_H

#include "stream.h"
#include <assert.h>

namespace flux
{
    //------------------------------------------------------------------------
    // Observer
    //------------------------------------------------------------------------
    template <class T, class Cb>
    class Observer : public Stream<T>
    {
    public:
        typedef Observer<T,Cb> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

        typedef typename my_t::base_handle_t base_handle_t;

    private:
        base_handle_t stream;
        Cb cb;

    public:
        explicit Observer(Cb const & cb = Cb()) :
            cb(cb)
        {}

        void pipeFrom(base_handle_t const & input)
        {
            stream = input;
        }

        void pipeTo(base_handle_t const & output)
        {
            stream = output;
        }

        bool get(T & x)
        {
            assert(stream);
            if(stream->get(x))
            {
                cb(*this, x);
                return true;
            }
            else
                return false;
        }

        void put(T const & x)
        {
            cb(*this, x);
            if(stream)
                stream->put(x);
        }

        bool fetch()
        {
            if(stream)
                return stream->fetch();
            else
                return false;
        }

        void flush()
        {
            if(stream)
                stream->flush();
        }
    };

    template <class T, class Cb>
    typename Observer<T,Cb>::handle_t makeObserver(Cb const & cb)
    {
        typename Observer<T,Cb>::handle_t s(new Observer<T,Cb>(cb));
        return s;
    }
}

#endif // FLUX_OBSERVER_H
