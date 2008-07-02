//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/generator.h#1 $
//
// Created 2006/01/20
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_GENERATOR_H
#define WARP_GENERATOR_H

#include "ex/exception.h"
#include <boost/shared_ptr.hpp>

namespace warp
{
    template <class T>
    class Generator
    {
    public:
        typedef T value_t;
        typedef Generator<T> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

    public:
        virtual ~Generator() {}

        /// Return the next item in the sequence.  May throw
        /// StopIteration if sequence is exhausted.
        virtual T next() = 0;

        /// Get the next item in sequence.  Returns true if item
        /// exists, or false on end of sequence (i.e. StopIteration
        /// was thrown).
        bool getNext(T & x)
        {
            try {
                x = next();
                return true;
            }
            catch(ex::StopIteration) {
                return false;
            }
        }
    };
}

#endif // WARP_GENERATOR_H
