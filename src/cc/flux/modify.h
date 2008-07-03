//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
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
#ifndef FLUX_MODIFY_H
#define FLUX_MODIFY_H

#include "stream.h"
#include <functional>
#include <assert.h>

// A stream type that takes a arbitrary modification operator
// with one input reference and one output reference as parameters.

namespace flux
{
    template<class T>
    struct FakeModify
    {
        void operator()(T & item) const
        {
            return;
        }
    };

    //------------------------------------------------------------------------
    // Modify
    //------------------------------------------------------------------------
    template <class T, class ModifyOp>
    class Modify : public Stream<T>
    {
    public:
        typedef Modify<T,ModifyOp> my_t;
        typedef boost::shared_ptr<my_t> handle_t;
        typedef typename my_t::base_handle_t base_handle_t;

    private:
        base_handle_t stream;
        bool isOutput;
        ModifyOp modifier;

    public:
        explicit Modify(ModifyOp const & modifier) :
            isOutput(false),
            modifier(modifier)
        {
        }

        void pipeFrom(base_handle_t const & input)
        {
            isOutput = false;
            stream = input;
        }

        void pipeTo(base_handle_t const & output)
        {
            isOutput = true;
            stream = output;
        }

        bool get(T & x)
        {
            assert(stream && !isOutput);
            if(!stream->get(x))
                return false;
            modifier(x);
            return true;
        }

        void put(T const & x)
        {
            assert(stream && isOutput);
            modifier(const_cast<T&>(x));
            stream->put(x);
        }

        void flush()
        {
            assert(stream && isOutput);
            stream->flush();
        }
    };

    template <class T, class ModifyOp>
    typename Modify<T,ModifyOp>::handle_t makeModify(ModifyOp const & modifier)
    {
        typename Modify<T,ModifyOp>::handle_t s(new Modify<T,ModifyOp>(modifier));
        return s;
    }

};

#endif // FLUX_MODIFY_H
