//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/flux/sequence.h#1 $
//
// Created 2006/05/01
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef FLUX_SEQUENCE_H
#define FLUX_SEQUENCE_H

#include "stream.h"
#include "warp/util.h"
#include <boost/shared_ptr.hpp>

namespace flux
{
    //------------------------------------------------------------------------
    // Sequence
    //------------------------------------------------------------------------
    /// Provide a Stream interface over a STL Sequence type.  Calls to
    /// put() insert at the position given by the \c GetPos functor
    /// (default is back insertion).  Calls to get() remove elements
    /// from the position given by the \c PutPos functor (default is
    /// front retrieval).  For certain types (e.g. vector)
    /// front retrieval can be an expensive operation.  Note that the
    /// sequence data is owned by this class; it's not just a thin
    /// wrapper over an external sequence.
    /// @param Seq STL-style sequence type.
    /// @param GetPos Functor returning sequence position for calls to
    /// get().  Default uses begin().
    /// @param PosPos Functor returning sequence position for calls to
    /// put().  Default uses end().
    template <class Seq,
              class GetPos=warp::begin_pos<Seq>,
              class PutPos=warp::end_pos<Seq> >
    class Sequence : public Stream<typename Seq::value_type>, public Seq
    {
    public:
        typedef Sequence<Seq,GetPos,PutPos> my_t;
        typedef Seq seq_t;
        typedef boost::shared_ptr<my_t> handle_t;

        typedef typename my_t::base_handle_t base_handle_t;
        typedef typename my_t::value_type value_type;
        typedef typename my_t::size_type size_type;
        typedef typename my_t::iterator iterator;

    private:
        GetPos get_pos;
        PutPos put_pos;

    public:
        explicit Sequence(GetPos const & get_pos = GetPos(),
                          PutPos const & put_pos = PutPos()) :
            get_pos(get_pos), put_pos(put_pos)
        {
        }

        explicit Sequence(seq_t const & o) :
            seq_t(o)
        {
        }

        explicit Sequence(size_type n, value_type const & t=value_type()) :
            seq_t(n,t)
        {
        }

        template <class It>
        Sequence(It first, It last) :
            seq_t(first, last)
        {
        }

        virtual bool get(value_type & x)
        {
            if(seq_t::empty())
                return false;

            iterator it = get_pos(*this);
            x = *it;
            erase(it);

            return true;
        }

        virtual void put(value_type const & x)
        {
            insert(put_pos(*this), x);
        }
    };
}

#endif // FLUX_SEQUENCE_H
