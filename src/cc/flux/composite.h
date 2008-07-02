//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/flux/composite.h#1 $
//
// Created 2006/03/02
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef FLUX_COMPOSITE_H
#define FLUX_COMPOSITE_H

#include "stream.h"

namespace flux
{
    //------------------------------------------------------------------------
    // Composite
    //------------------------------------------------------------------------
    template <class T>
    class Composite : public Stream<T>
    {
    public:
        typedef Composite<T> my_t;
        typedef boost::shared_ptr<my_t> handle_t;
        typedef typename my_t::base_handle_t base_handle_t;

    protected:
        Composite() {}
        
    public:
        virtual bool get(int idx, T & x)
        {
            ex::raise<StreamError>("get from non-input stream");
        }
        virtual void put(int idx, T const & x)
        {
            ex::raise<StreamError>("put to non-output stream");
        }

        inline base_handle_t getChannel(int idx);
    };
    

    //------------------------------------------------------------------------
    // CompositeChannel
    //------------------------------------------------------------------------
    template <class T>
    class CompositeChannel : public Stream<T>
    {
    public:
        typedef CompositeChannel<T> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

    private:
        typedef typename Composite<T>::handle_t comp_handle_t;
        
        comp_handle_t composite;
        int channel;

    public:
        CompositeChannel(comp_handle_t const & composite, int channel) :
            composite(composite), channel(channel)
        {
            assert(composite);
        }

        bool get(T & x)
        {
            return composite->get(channel, x);
        }

        void put(T const & x)
        {
            composite->put(channel, x);
        }

        bool fetch()
        {
            return composite->fetch();
        }
        
        void flush()
        {
            composite->flush();
        }
    };

    template <class T>
    typename CompositeChannel<T>::handle_t makeCompositeChannel(
        typename Composite<T>::handle_t const & composite, int channel)
    {
        typename CompositeChannel<T>::handle_t s
            (new CompositeChannel<T>(composite, channel));
        return s;
    }

    template <class T>
    typename Composite<T>::base_handle_t Composite<T>::getChannel(int idx)
    {
        return makeCompositeChannel<T>(
            boost::static_pointer_cast<my_t>(my_t::shared_from_this()), idx);
    }
}

#endif // FLUX_COMPOSITE_H
