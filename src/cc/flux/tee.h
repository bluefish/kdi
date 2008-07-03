//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-04-28
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
