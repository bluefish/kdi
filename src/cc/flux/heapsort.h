//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-11
// 
// This file is part of the flux library.
// 
// The flux library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The flux library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef FLUX_HEAPSORT_H
#define FLUX_HEAPSORT_H

#include <flux/stream.h>
#include <warp/util.h>
#include <vector>
#include <functional>
#include <algorithm>
#include <assert.h>

namespace flux
{
    //------------------------------------------------------------------------
    // HeapSort
    //------------------------------------------------------------------------
    template <class T, class Lt=std::less<T> >
    class HeapSort : public Stream<T>
    {
    public:
        typedef HeapSort<T,Lt> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

        typedef typename my_t::base_handle_t base_handle_t;

    private:
        typedef std::vector<T> value_vec_t;

        value_vec_t buf;
        base_handle_t output;
        warp::reverse_order<T,Lt> lt;
        
    public:
        explicit HeapSort(Lt const & lt=Lt()) :
            lt(lt) {}

        ~HeapSort()
        {
            flushOutputBuffer();
        }

        void pipeTo(base_handle_t const & output)
        {
            flushOutputBuffer();
            this->output = output;
        }

        void put(T const & x)
        {
            buf.push_back(x);

            T * begin = &buf[0];
            T * end = begin + buf.size();
            std::push_heap(begin, end, lt);
        }

        void flushOutputBuffer()
        {
            T * begin = &buf[0];
            T * end = begin + buf.size();
            while(begin != end)
            {
                assert(output);
                output->put(*begin);
                std::pop_heap(begin, end, lt);
                buf.pop_back();
                --end;
            }
        }

        void flush()
        {
            assert(output);
            flushOutputBuffer();
            output->flush();
        }
    };

    template <class T>
    typename HeapSort<T>::handle_t makeHeapSort()
    {
        typename HeapSort<T>::handle_t s(new HeapSort<T>());
        return s;
    }

    template <class T, class Lt>
    typename HeapSort<T,Lt>::handle_t makeHeapSort(Lt const & lt)
    {
        typename HeapSort<T,Lt>::handle_t s(new HeapSort<T,Lt>(lt));
        return s;
    }
}

#endif // FLUX_HEAPSORT_H
