//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-02-21
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

#ifndef OORT_ORDERMGR_H
#define OORT_ORDERMGR_H

#include "record.h"
#include "typeregistry.h"
#include "warp/singleton.h"
#include "ex/exception.h"
#include <boost/shared_ptr.hpp>

namespace oort
{
    namespace vega {
        struct HalfHash;
        struct UrlHash;
    }

    typedef bool (OrderFunc)(Record const &, Record const &);
    typedef vega::HalfHash const & (HostKeyFunc)(Record const &);
    typedef vega::UrlHash const & (UrlKeyFunc)(Record const &);
    
    class OrderManager;
    class HostKeyManager;
    class UrlKeyManager;

    class NaturalOrder;

    template <class Key, class KeyMgr>
    class KeyOrder;

    typedef KeyOrder<vega::HalfHash, HostKeyManager> HostKeyOrder;
    typedef KeyOrder<vega::UrlHash,  UrlKeyManager>  UrlKeyOrder;
}


//----------------------------------------------------------------------------
// OrderManager
//----------------------------------------------------------------------------
class oort::OrderManager : public TypeRegistry<OrderFunc *>,
                           public warp::Singleton<OrderManager>
{
    friend class warp::Singleton<OrderManager>;
    OrderManager() :
        TypeRegistry<OrderFunc *>("oort::OrderManager")
    {
    }

    template <class T> struct RegHelper
    {
        static bool naturalOrder(Record const & a, Record const & b)
        {
            return *(a.cast<T>()) < *(b.cast<T>());
        }
    };

public:
    template <class T> void regOrder()
    {
        registerType<T>(&RegHelper<T>::naturalOrder);
    }
};


//----------------------------------------------------------------------------
// HostKeyManager
//----------------------------------------------------------------------------
class oort::HostKeyManager : public TypeRegistry<HostKeyFunc *>,
                             public warp::Singleton<HostKeyManager>
{
    friend class warp::Singleton<HostKeyManager>;
    HostKeyManager() :
        TypeRegistry<HostKeyFunc *>("oort::HostKeyManager")
    {
    }

    template <class T> struct RegHelper
    {
        static vega::HalfHash const & getHostKey(Record const & r)
        {
            return r.cast<T>()->getHostHash();
        }
    };

public:
    typedef HostKeyFunc keyfunc_t;
    typedef vega::HalfHash key_t;

    template <class T> void regHostKey()
    {
        registerType<T>(&RegHelper<T>::getHostKey);
    }
};


//----------------------------------------------------------------------------
// UrlKeyManager
//----------------------------------------------------------------------------
class oort::UrlKeyManager : public TypeRegistry<UrlKeyFunc *>,
                            public warp::Singleton<UrlKeyManager>
{
    friend class warp::Singleton<UrlKeyManager>;
    UrlKeyManager() :
        TypeRegistry<UrlKeyFunc *>("oort::UrlKeyManager")
    {
    }

    template <class T> struct RegHelper
    {
        static vega::UrlHash const & getUrlKey(Record const & r)
        {
            return r.cast<T>()->getUrlHash();
        }
    };

public:
    typedef UrlKeyFunc keyfunc_t;
    typedef vega::UrlHash key_t;

    template <class T> void regUrlKey()
    {
        registerType<T>(&RegHelper<T>::getUrlKey);
    }
};


//----------------------------------------------------------------------------
// NaturalOrder
//----------------------------------------------------------------------------
class oort::NaturalOrder
{
    mutable OrderFunc * orderFunc;
    mutable uint32_t orderType;
    mutable uint32_t orderVer;

public:
    NaturalOrder() : orderFunc(0) {}

    bool operator()(Record const & a, Record const & b) const
    {
        uint32_t atype = a.getType();
        uint32_t btype = b.getType();
        uint32_t aver = a.getVersion();
        uint32_t bver = b.getVersion();

        assert(atype == btype && aver == bver);

        if(atype != btype)
            ex::raise<TypeError>("can't order different types: "
                                 "typeA='%s', typeB='%s'",
                                 warp::typeString(atype).c_str(),
                                 warp::typeString(btype).c_str());
        else if(aver != bver)
            ex::raise<VersionError>("can't order different versions of "
                                    "type '%s': verA=%u, verB=%u",
                                    warp::typeString(atype).c_str(),
                                    aver, bver);
        else if(!orderFunc || orderType != atype || orderVer != aver)
        {
            orderFunc = OrderManager::get().lookupType(atype, aver);
            orderType = atype;
            orderVer = aver;
        }
        
        return (*orderFunc)(a, b);
    }
};


//----------------------------------------------------------------------------
// KeyOrder
//----------------------------------------------------------------------------
template <class Key, class KeyMgr>
class oort::KeyOrder
{
public:
    bool operator()(Record const & a, Record const & b) const
    {
        KeyMgr & km = KeyMgr::get();
        return ( (*km.lookupType(a.getType(), a.getVersion()))(a) <
                 (*km.lookupType(b.getType(), b.getVersion()))(b) );
    }

    bool operator()(Record const & a, Key const & b) const
    {
        KeyMgr & km = KeyMgr::get();
        return (*km.lookupType(a.getType(), a.getVersion()))(a) < b;
    }

    bool operator()(Key const & a, Record const & b) const
    {
        KeyMgr & km = KeyMgr::get();
        return a < (*km.lookupType(b.getType(), b.getVersion()))(b);
    }

    bool operator()(Key const & a, Key const & b) const
    {
        return a < b;
    }
};

#endif // OORT_ORDERMGR_H
