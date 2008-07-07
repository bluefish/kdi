//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-08-17
// 
// This file is part of the warp library.
// 
// The warp library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The warp library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef WARP_HASHMAP_H
#define WARP_HASHMAP_H

#include <warp/hashtable.h>
#include <warp/hash.h>
#include <warp/util.h>
#include <ex/exception.h>
#include <boost/type_traits/is_same.hpp>
#include <boost/utility/enable_if.hpp>
#include <vector>

namespace warp
{
    template <class K, class V,
              class KHash = warp::hash<K>,
              class H = typename warp::result_of_hash<KHash>::type,
              class KEq = warp::equal_to>
    class HashMap;
}

//----------------------------------------------------------------------------
// HashMap
//----------------------------------------------------------------------------
template <class K, class V, class KHash, class H, class KEq>
class warp::HashMap
{
    typedef HashMap<K,V,KHash,H,KEq> my_t;

    typedef K key_t;
    typedef V value_t;
    typedef KHash key_hash_t;
    typedef H hash_result_t;
    typedef KEq key_equal_t;

public:
    struct item_t
    {
        key_t key;
        value_t value;

        item_t() {}
        item_t(key_t const & key, value_t const & val) :
            key(key), value(val) {}
    };

private:
    static key_t const & key_of(item_t const & x) { return x.key; }

    template <class T> static
    typename boost::disable_if<boost::is_same<T, item_t>, T>::type const &
    key_of(T const & x) { return x; }

    struct item_hash_t
    {
        key_hash_t h;

        template <class T>
        hash_result_t operator()(T const & x) const {
            return h(key_of(x));
        }
    };

    struct item_equal_t
    {
        key_equal_t eq;

        template <class A, class B>
        bool operator()(A const & a, B const & b) const {
            return eq(key_of(a), key_of(b));
        }
    };

    typedef warp::HashTable<item_t, item_hash_t, hash_result_t, item_equal_t> table_t;

    table_t tbl;

public:
    /// Set a value for the given key, overwriting any previous value
    /// for the key.
    void set(key_t const & key, value_t const & val)
    {
        tbl.insertOverwrite(item_t(key, val));
    }

    /// Check to see if the table contains the given key.
    template <class KK>
    bool contains(KK const & key) const
    {
        return tbl.contains(key);
    }
    

    /// Get the value associated with \c key.
    /// @throws KeyError if key is not in table
    template <class KK>
    value_t const & get(KK const & key) const
    {
        using namespace ex;
        typename table_t::const_iterator i = tbl.find(key);
        if(i == tbl.end())
            raise<KeyError>("%1%", key);
        else
            return i->value;
    }

    /// Get the value associated with \c key.  If the key is not in
    /// the table, return the given default value.
    template <class KK>
    value_t const & get(KK const & key, value_t const & dft) const
    {
        typename table_t::const_iterator i = tbl.find(key);
        if(i == tbl.end())
            return dft;
        else
            return i->value;
    }

    /// Get a reference to the value associated with \c key.
    /// @throws KeyError if key is not in table
    template <class KK>
    value_t & get(KK const & key)
    {
        using namespace ex;
        typename table_t::const_iterator i = tbl.find(key);
        if(i == tbl.end())
            raise<KeyError>("%1%", key);
        else
            // This works because the value does not figure into the
            // hash ordering
            return const_cast<value_t &>(i->value);
    }

    /// Get a reference to the value associated with \c key.  If the
    /// key is not in the table, insert it with the given default
    /// value.
    template <class KK>
    value_t & setdefault(KK const & key, value_t const & dft)
    {
        typename table_t::const_iterator i = tbl.find(key);
        if(i == tbl.end())
        {
            tbl.insert(item_t(key, dft));
            i = tbl.find(key);
        }
        
        // This works because the value does not figure into the hash
        // ordering
        return const_cast<value_t &>(i->value);
    }

    /// Clear out all keys and values in the table.
    void clear()
    {
        tbl.clear();
    }

    /// Rebuild the current hash map using the given load factor as a
    /// target.
    void rebuildToLoad(double targetLoad)
    {
        tbl.rebuildToLoad(targetLoad);
    }

    /// Get number of items in the hash map.
    size_t size() const { return tbl.size(); }

    /// Swap contents of two hash maps.
    void swap(my_t & o)
    {
        tbl.swap(o.tbl);
    }

    /// Get a vector of all the keys in the map
    std::vector<key_t> getKeys() const
    {
        std::vector<key_t> keys;
        keys.reserve(size());
        for(typename table_t::const_iterator i = tbl.begin();
            i != tbl.end(); ++i)
        {
            if(i.exists())
                keys.push_back(i->key);
        }
        return keys;
    }
    
public:
    typedef typename table_t::const_iterator const_iterator;

    const_iterator begin() const { return tbl.begin(); }
    const_iterator end() const { return tbl.end(); }
};


#endif // WARP_HASHMAP_H
