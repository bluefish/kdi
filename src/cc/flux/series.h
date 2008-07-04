//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-02-14
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

#ifndef FLUX_SERIES_H
#define FLUX_SERIES_H

#include <flux/stream.h>
#include <ex/exception.h>
#include <queue>

namespace flux
{
    template <class T>
    class Series : public Stream<T>
    {
    public:
        typedef Series<T> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

        typedef typename my_t::base_handle_t base_handle_t;

    private:
        typedef std::queue<base_handle_t> queue_t;
        queue_t q;
        
    public:
        void pipeFrom(base_handle_t const & input)
        {
            using namespace ex;
            if(!input)
                raise<ValueError>("null input");

            q.push(input);
        }

        bool get(T & x)
        {
            while(!q.empty())
            {
                if(q.front()->get(x))
                    return true;
                else
                    q.pop();
            }
            return false;
        }
    };

    template <class T>
    typename Series<T>::handle_t makeSeries()
    {
        typename Series<T>::handle_t s(new Series<T>());
        return s;
    }
}

#endif // FLUX_SERIES_H
