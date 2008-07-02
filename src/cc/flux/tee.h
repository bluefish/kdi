//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: flux/tee.h $
//
// Created 2008/04/28
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef FLUX_TEE_H
#define FLUX_TEE_H

#include <flux/stream.h>
#include <vector>

namespace flux {

    template <class T>
    class Tee;

} // namespace flux

//----------------------------------------------------------------------------
// Tee
//----------------------------------------------------------------------------
template <class T>
class flux::Tee : public flux::Stream<T>
{
public:
    typedef flux::Tee<T> my_t;
    typedef flux::Stream<T> super;
    typedef boost::shared_ptr<my_t> handle_t;
    typedef typename super::base_handle_t base_handle_t;

private:
    typedef std::vector<base_handle_t> vec_t;
    vec_t outputs;

public:
    void pipeTo(base_handle_t const & output)
    {
        if(output)
            outputs.push_back(output);
    }

    void put(T const & x)
    {
        for(typename vec_t::const_iterator it = outputs.begin();
            it != outputs.end(); ++it)
        {
            (*it)->put(x);
        }
    }

    void flush()
    {
        for(typename vec_t::const_iterator it = outputs.begin();
            it != outputs.end(); ++it)
        {
            (*it)->flush();
        }
    }
};

//----------------------------------------------------------------------------
// makeTee
//----------------------------------------------------------------------------
namespace flux {

    template <class T>
    typename Tee<T>::handle_t makeTee()
    {
        typename Tee<T>::handle_t h(new Tee<T>);
        return h;
    }

}



#endif // FLUX_TEE_H
