//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/limited_inserter.h $
//
// Created 2008/03/17
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_LIMITED_INSERTER_H
#define WARP_LIMITED_INSERTER_H

#include <stdexcept>
#include <iterator>

namespace warp {

    /// Implements an output iterator over a limited iterator range.
    /// If the range is exceeded, a std::out_of_range error is raised.
    template <class OutputIt>
    class limited_output_iterator
    {
    public:
        typedef std::output_iterator_tag iterator_category;
        typedef typename std::iterator_traits<OutputIt>::value_type value_type;
        typedef typename std::iterator_traits<OutputIt>::difference_type difference_type;
        typedef typename std::iterator_traits<OutputIt>::pointer pointer;
        typedef typename std::iterator_traits<OutputIt>::reference reference;

    private:
        typedef limited_output_iterator<OutputIt> my_t;
        
        OutputIt it;
        OutputIt end;

    public:
        limited_output_iterator() : it(), end() {}
        limited_output_iterator(OutputIt begin, OutputIt end) :
            it(begin), end(end) {}
        
        my_t & operator++()
        {
            if(it == end)
                throw std::out_of_range("iterator limit exceeded");
            ++it;
            return *this;
        }

        my_t operator++(int)
        {
            my_t o(*this);
            ++(*this);
            return o;
        }

        pointer operator->() const
        {
            if(it == end)
                throw std::out_of_range("iterator limit exceeded");
            return &*it;
        }

        reference operator*() const
        {
            if(it == end)
                throw std::out_of_range("iterator limit exceeded");
            return *it;
        }

        operator OutputIt() const
        {
            return it;
        }
    };

    /// Creates a limited_output_iterator over the given range.
    template <class It>
    limited_output_iterator<It> limited_inserter(It begin, It end)
    {
        return limited_output_iterator<It>(begin, end);
    }

} // namespace warp

#endif // WARP_LIMITED_INSERTER_H
