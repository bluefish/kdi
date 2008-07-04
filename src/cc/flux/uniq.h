//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-11
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

#ifndef FLUX_UNIQ_H
#define FLUX_UNIQ_H

#include <flux/stream.h>
#include <functional>
#include <assert.h>

namespace flux
{
    //------------------------------------------------------------------------
    // UniqKeepFirst
    //------------------------------------------------------------------------
    template <class T>
    struct UniqKeepFirst
    {
        void operator()(T & out, T const & in1, T const & in2)
        {
            out = in1;
        }
    };
    
    //------------------------------------------------------------------------
    // UniqKeepLast
    //------------------------------------------------------------------------
    template <class T>
    struct UniqKeepLast
    {
        void operator()(T & out, T const & in1, T const & in2)
        {
            out = in2;
        }
    };

    //------------------------------------------------------------------------
    // Uniq
    //------------------------------------------------------------------------
    template <class T,
              class Lt=std::less<T>,
              class KeepPolicy=UniqKeepFirst<T> >
    class Uniq : public Stream<T>
    {
    public:
        typedef Uniq<T,Lt,KeepPolicy> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

        typedef typename my_t::base_handle_t base_handle_t;

    private:
        T heldValue;
        base_handle_t stream;
        bool holdingValue;
        bool isOutput;

        Lt lt;
        KeepPolicy keepPolicy;

    public:
        explicit Uniq(Lt const & lt = Lt(),
                      KeepPolicy const & keepPolicy = KeepPolicy()) :
            holdingValue(false),
            isOutput(false),
            lt(lt),
            keepPolicy(keepPolicy)
        {
        }

        ~Uniq()
        {
            if(isOutput && holdingValue)
            {
                assert(stream);
                stream->put(heldValue);
            }
        }

        void pipeFrom(base_handle_t const & input)
        {
            if(holdingValue)
            {
                if(isOutput)
                {
                    assert(stream);
                    stream->put(heldValue);
                }
                holdingValue = false;
                heldValue = T();
            }
            isOutput = false;
            stream = input;
        }

        void pipeTo(base_handle_t const & output)
        {
            if(holdingValue)
            {
                if(isOutput)
                {
                    assert(stream);
                    stream->put(heldValue);
                }
                holdingValue = false;
                heldValue = T();
            }
            isOutput = true;
            stream = output;
        }

        bool get(T & x)
        {
            assert(stream && !isOutput);
            
            for(;;)
            {
                if(!holdingValue)
                {
                    if(!stream->get(heldValue))
                        return false;
                    else
                        holdingValue = true;
                }
                else
                {
                    T tmp;
                    if(stream->get(tmp))
                    {
                        if(lt(heldValue, tmp) || lt(tmp, heldValue))
                        {
                            x = heldValue;
                            heldValue = tmp;
                            return true;
                        }
                        else
                            keepPolicy(heldValue, heldValue, tmp);
                    }
                    else
                    {
                        x = heldValue;
			heldValue = T();
                        holdingValue = false;
                        return true;
                    }
                }
            }
        }

        void put(T const & x)
        {
            assert(stream && isOutput);
            
            if(holdingValue)
            {
                if(lt(heldValue, x) || lt(x, heldValue))
                {
                    stream->put(heldValue);
                    heldValue = x;
                }
                else
                    keepPolicy(heldValue, heldValue, x);
            }
            else
            {
                heldValue = x;
                holdingValue = true;
            }
        }

        void flush()
        {
            assert(stream && isOutput);

            if(holdingValue)
            {
                stream->put(heldValue);
		heldValue = T();
                holdingValue = false;
            }
            stream->flush();
        }
    };


    template <class T>
    typename Uniq<T>::handle_t makeUniq()
    {
        typename Uniq<T>::handle_t s(new Uniq<T>());
        return s;
    }

    template <class T, class Lt>
    typename Uniq<T,Lt>::handle_t makeUniq(Lt const & lt)
    {
        typename Uniq<T,Lt>::handle_t s(new Uniq<T,Lt>(lt));
        return s;
    }

    template <class T, class Lt, class Kp>
    typename Uniq<T,Lt,Kp>::handle_t makeUniq(Lt const & lt, Kp const & kp)
    {
        typename Uniq<T,Lt,Kp>::handle_t s(new Uniq<T,Lt,Kp>(lt,kp));
        return s;
    }
}

#endif // FLUX_UNIQ_H
