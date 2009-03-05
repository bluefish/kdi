//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/Iterator.h $
//
// Created 2009/03/02
//
// Copyright 2009 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_ITERATOR_H
#define WARP_ITERATOR_H

namespace warp {

    template <class T>
    class Iterator
    {
    public:
        virtual ~Iterator() {}

        /// Move to the next element in the sequence.  Return true iff
        /// such an element exists, or false on end-of-sequence.
        virtual bool next() = 0;

        /// Get the current element in the sequence.  This method is only
        /// valid when the last call to next() has returned true.  If
        /// next() last returned false or hasn't yet been called, the
        /// current element is undefined.
        virtual T const & getCurrent() const = 0;
    };

} // namespace warp

#endif // WARP_ITERATOR_H
