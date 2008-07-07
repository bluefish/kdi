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

#ifndef FLUX_SORT_H
#define FLUX_SORT_H

#include <flux/stream.h>
#include <vector>
#include <functional>
#include <algorithm>
#include <assert.h>

namespace flux
{
    //------------------------------------------------------------------------
    // Sort
    //------------------------------------------------------------------------
    template <class T, class Lt=std::less<T> >
    class Sort : public Stream<T>
    {
        // Usage:
        //
        //   Sort<T, Lt> sorter(lt);
        //   sorter.setOutput(output);
        //   
        //   T r;
        //   uint32_t approxMemUsage = 0;  // Input buffering scheme makes 
        //                                 // exact mem usage tracking hard
        //   while(input.get(r))
        //   {
        //       approxMemUsage += calcMemUsage(r);
        //       sorter.put(r);
        //       
        //       if(approxMemUsage >= sortThreshold)
        //           sorter.flush();
        //       approxMemUsage = 0;
        //   }
        //   sorter.flush();

    public:
        typedef Sort<T,Lt> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

        typedef typename my_t::base_handle_t base_handle_t;

    private:
        typedef std::vector<T> value_vec_t;

        value_vec_t buf;
        base_handle_t output;
        Lt lt;
        
    public:
        explicit Sort(Lt const & lt=Lt()) :
            lt(lt) {}

        ~Sort()
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
        }

        void flushOutputBuffer()
        {
	    if(!buf.empty())
	    {
	        T * begin = &buf[0];
	        T * end = begin + buf.size();
	        std::sort(begin, end, lt);

                assert(output);
                for(typename value_vec_t::const_iterator ii = buf.begin();
                    ii != buf.end(); ++ii)
                {
                    output->put(*ii);
                }

                buf.clear();
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
    typename Sort<T>::handle_t makeSort()
    {
        typename Sort<T>::handle_t s(new Sort<T>());
        return s;
    }

    template <class T, class Lt>
    typename Sort<T,Lt>::handle_t makeSort(Lt const & lt)
    {
        typename Sort<T,Lt>::handle_t s(new Sort<T,Lt>(lt));
        return s;
    }
}

#endif // FLUX_SORT_H
