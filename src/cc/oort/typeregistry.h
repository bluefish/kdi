//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/oort/typeregistry.h#1 $
//
// Created 2006/02/21
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef OORT_TYPEREGISTRY_H
#define OORT_TYPEREGISTRY_H

#include "warp/strutil.h"
#include "exception.h"

#include <boost/utility.hpp>
#include <vector>
#include <algorithm>
#include <utility>
#include <string>
#include <stdint.h>

namespace oort
{
    template <class RegType>
    class TypeRegistry;
}

//----------------------------------------------------------------------------
// TypeRegistry
//----------------------------------------------------------------------------
template <class ValueType>
class oort::TypeRegistry : private boost::noncopyable
{
public:
    typedef std::pair<uint32_t,uint32_t> key_t;
    typedef ValueType value_t;

private:
    typedef std::pair<key_t, value_t> item_t;
    typedef std::vector<item_t> vec_t;

    struct KeyLt
    {
        bool operator()(item_t const & a, item_t const & b) { return a.first < b.first; }
        bool operator()(item_t const & a, key_t const & b)  { return a.first < b; }
        bool operator()(key_t const & a, item_t const & b)  { return a < b.first; }
        bool operator()(key_t const & a, key_t const & b)   { return a < b; }
    };

    vec_t reg;
    std::string regName;

public:
    explicit TypeRegistry(std::string const & regName) : regName(regName) {}

    /// Register a value for a (type, version) pair.  Registration
    /// must be unique.  Throws an exception on conflicts.
    void registerType(uint32_t rtype, uint32_t rver, value_t const & val)
    {
        using namespace ex;
        using namespace warp;

        key_t k(rtype, rver);
        typename vec_t::iterator ii =
            std::lower_bound(reg.begin(), reg.end(), k, KeyLt());
        if(ii != reg.end() && ii->first == k)
            raise<TypeError>("%s registration conflict: type='%s', ver=%u",
                             regName, typeString(rtype), rver);

        reg.insert(ii, item_t(k, val));
    }

    /// Look up a value for a previously registered (type, version)
    /// pair.  Throws an exception if the pair wasn't previously
    /// registered.
    value_t const & lookupType(uint32_t rtype, uint32_t rver) const
    {
        using namespace ex;
        using namespace warp;

        key_t k(rtype, rver);
        typename vec_t::const_iterator ii =
            std::lower_bound(reg.begin(), reg.end(), k, KeyLt());
        if(ii != reg.end() && ii->first == k)
            return ii->second;
        else
            raise<TypeError>("%s unregistered type: type='%s', ver=%u",
                             regName, typeString(rtype), rver);
    }

    /// Shortcut for registering an Oort record type.
    template <class T>
    void registerType(value_t const & val)
    {
        registerType(T::TYPECODE, T::VERSION, val);
    }

    /// Shortcut for looking up an Oort record type.
    template <class T>
    value_t const & lookupType() const
    {
        return lookupType(T::TYPECODE, T::VERSION);
    }
};


#endif // OORT_TYPEREGISTRY_H
