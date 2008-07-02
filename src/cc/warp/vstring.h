//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/vstring.h#1 $
//
// Created 2006/06/07
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_VSTRING_H
#define WARP_VSTRING_H

#include "ex/exception.h"
#include "strutil.h"
#include <vector>
#include <string>
#include <iostream>

namespace warp
{
    class VString;
}

//----------------------------------------------------------------------------
// VString
//----------------------------------------------------------------------------
class warp::VString : public std::vector<char>
{
    typedef std::vector<char> super;
    typedef warp::VString my_t;

public:
    VString() {}

    explicit VString(super const & o) : super(o) {}
    explicit VString(std::string const & s) : super(s.begin(), s.end()) {}
    explicit VString(str_data_t const & s) : super(s.begin(), s.end()) {}

    template <class It>
    VString(It first, It last) : super(first, last) {}

    // Assignment
    my_t & operator=(super const & o)
    {
        super::operator=(o);
        return *this;
    }

    my_t & operator=(str_data_t const & s)
    {
        clear();
        insert(end(), s.begin(), s.end());
        return *this;
    }

    my_t & operator=(std::string const & s)
    {
        clear();
        insert(end(), s.begin(), s.end());
        return *this;
    }

    my_t & operator=(char const * cstr)
    {
        clear();
        while(*cstr)
            push_back(*cstr++);
        return *this;
    }

    // Append
    void append(char const * first, char const * last)
    {
        insert(end(), first, last);
    }

    my_t & operator+=(super const & s)
    {
        insert(end(), s.begin(), s.end());
        return *this;
    }

    my_t & operator+=(str_data_t const & s)
    {
        insert(end(), s.begin(), s.end());
        return *this;
    }

    my_t & operator+=(std::string const & s)
    {
        insert(end(), s.begin(), s.end());
        return *this;
    }

    my_t & operator+=(char const * cstr)
    {
        while(*cstr)
            push_back(*cstr++);
        return *this;
    }

    my_t & operator+=(char c)
    {
        push_back(c);
        return *this;
    }

    // Comparison
    bool operator<(super const & o) const
    {
        return std::lexicographical_compare(
            begin(), end(), o.begin(), o.end());
    }

    bool operator<(str_data_t const & o) const
    {
        return std::lexicographical_compare(
            begin(), end(), o.begin(), o.end());
    }

    bool operator<(std::string const & o) const
    {
        return std::lexicographical_compare(
            begin(), end(), o.begin(), o.end());
    }

    bool operator<(char const * cStr) const
    {
        return std::lexicographical_compare(
            begin(), end(), cStr, cStr + strlen(cStr));
    }

    bool operator==(super const & o) const
    {
        return size() == o.size() &&
            std::equal(begin(), end(), o.begin());
    }

    bool operator==(str_data_t const & o) const
    {
        return size() == o.size() &&
            std::equal(begin(), end(), o.begin());
    }

    bool operator==(std::string const & o) const
    {
        return size() == o.size() &&
            std::equal(begin(), end(), o.begin());
    }

    bool operator==(char const * cStr) const
    {
        size_t len = strlen(cStr);
        return size() == len &&
            std::equal(begin(), end(), cStr);
    }

    bool operator!=(super const & o) const
    {
        return !(*this == o);
    }

    bool operator!=(str_data_t const & o) const
    {
        return !(*this == o);
    }

    bool operator!=(std::string const & o) const
    {
        return !(*this == o);
    }

    bool operator!=(char const * cStr) const
    {
        return !(*this == cStr);
    }

    template <class Int>
    void appendInt(Int x)
    {
        size_t p = size();

        bool neg;
        if(x < 0)
        {
            neg = true;
            x = -x;
        }
        else
            neg = false;

        do {
            push_back('0' + x % 10);
            x /= 10;
        } while(x);

        if(neg)
            push_back('-');

        std::reverse(begin()+p, end());
    }
    template <class Int>
    void setInt(Int x)
    {
        clear();
        appendInt(x);
    }
};


//----------------------------------------------------------------------------
// Free functions
//----------------------------------------------------------------------------
namespace warp
{
    // ostream operator    
    inline std::ostream & operator<<(std::ostream & out, VString const & s)
    {
        if(!s.empty())
            out.write(&s[0], s.size());
        return out;
    }

    /// Wrap a VString object.  NB: you're asking for a segfault if
    /// you try to wrap a temporary/immediate string, so don't do
    /// that.
    inline str_data_t wrap(VString const & s)
    {
        char const * p = &s[0];
        return str_data_t(p, p + s.size());
    }

    inline bool operator==(str_data_t const & a, VString const & b)
    {
        return b == a;
    }

    inline bool operator!=(str_data_t const & a, VString const & b)
    {
        return b != a;
    }

    inline bool operator==(std::string const & a, VString const & b)
    {
        return b == a;
    }

    inline bool operator!=(std::string const & a, VString const & b)
    {
        return b != a;
    }
}


#endif // WARP_VSTRING_H
