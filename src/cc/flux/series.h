//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/flux/series.h#1 $
//
// Created 2007/02/14
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef FLUX_SERIES_H
#define FLUX_SERIES_H

#include "stream.h"
#include "ex/exception.h"
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
