//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-05-12
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

#ifndef WARP_HASHTABLE_H
#define WARP_HASHTABLE_H

#include <warp/hash.h>
#include <warp/util.h>
#include <warp/prime.h>
#include <ex/exception.h>
#include <vector>
#include <utility>
#include <functional>
#include <algorithm>
#include <math.h>

namespace warp
{
    template <class T,
              class H = warp::hash<T>,
              class HK = typename warp::result_of_hash<H>::type,
              class TEq = warp::equal_to>
    class HashTable;
}


//----------------------------------------------------------------------------
// HashTable
//----------------------------------------------------------------------------
template <class T, class H, class HK, class TEq>
class warp::HashTable
{
    // hash_t : item_t -> hashkey_t
public:
    typedef T item_t;
    typedef H hash_t;
    typedef TEq equal_t;
    typedef HK hashkey_t;

private:
    typedef HashTable<T,H,HK,TEq> my_t;
    typedef std::pair<hashkey_t, item_t> value_t;
    typedef std::vector<value_t> vec_t;
    typedef typename vec_t::iterator iter_t;
    typedef typename vec_t::const_iterator citer_t;

    size_t nItems;              // current item count
    size_t loadThreshold;       // max items before resize
    double loadFactor;          // cap * factor = threshold
    vec_t table;
    hash_t hash;
    equal_t equals;

    static hashkey_t remapKey()   { return hashkey_t(0); }
    static hashkey_t emptyKey()   { return hashkey_t(-1); }
    static hashkey_t deletedKey() { return hashkey_t(-2); }

    void grow(size_t newCap)
    {
        assert(newCap >= nItems);

        newCap = nextPrime(newCap);
        vec_t newTable(newCap, value_t(emptyKey(), item_t()));
        if(nItems)
        {
            iter_t p0 = newTable.begin();
            iter_t p1 = newTable.end();

            for(iter_t i = table.begin(); i != table.end(); ++i)
            {
                if(i->first != emptyKey() && i->first != deletedKey())
                {
                    iter_t p = p0 + size_t(i->first) % newCap;
                    while(p->first != emptyKey())
                    {
                        if(++p == p1)
                            p = p0;
                    }
                    std::swap(*p, *i);
                }
            }
        }
        
        table.swap(newTable);
        loadThreshold = size_t(newCap * loadFactor);
    }

    template <class HashableType>
    hashkey_t hashItem(HashableType const & x) const
    {
        hashkey_t k = hash(x);
        if(k == emptyKey() || k == deletedKey())
            k = remapKey();
        return k;
    }

    iter_t probeAvailable(hashkey_t k)
    {
        assert(!table.empty());
        assert(nItems < table.size());

        iter_t p0 = table.begin();
        iter_t p1 = table.end();
        iter_t p = p0 + size_t(k) % table.size();
        while(p->first != emptyKey() && p->first != deletedKey())
        {
            if(++p == p1)
                p = p0;
        }
        return p;
    }

    citer_t probeAvailable(hashkey_t k) const
    {
        return const_cast<my_t *>(this)->probeAvailable(k);
    }

    template <class ComparableType>
    iter_t probeEqual(hashkey_t k, ComparableType const & x)
    {
        iter_t p0 = table.begin();
        iter_t p1 = table.end();

        if(p0 == p1)
            return p1;

        iter_t p = p0 + size_t(k) % table.size();
        iter_t start = p;
        
        for(;;)
        {
            if(p->first == emptyKey())
                return p1;

            if(p->first == k && equals(p->second, x))
                return p;
            
            if(++p == p1)
                p = p0;

            if(p == start)
                return p1;
        }
    }

    template <class ComparableType>
    citer_t probeEqual(hashkey_t k, ComparableType const & x) const
    {
        return const_cast<my_t *>(this)->probeEqual(k,x);
    }


public:
    explicit HashTable(double loadFactor = 0.75,
                       hash_t const & hash = hash_t(),
                       equal_t const & equals = equal_t()) :
        nItems(0), loadThreshold(0), loadFactor(loadFactor),
        hash(hash), equals(equals)
    {
        if(loadFactor <= 0.0 || loadFactor > 1.0)
            ex::raise<ex::RuntimeError>("invalid load factor: %f", loadFactor);
    }

    explicit HashTable(size_t sz,
                       double loadFactor = 0.75,
                       hash_t const & hash = hash_t(),
                       equal_t const & equals = equal_t()) :
        nItems(0), loadThreshold(0), loadFactor(loadFactor),
        hash(hash), equals(equals)
    {
        if(loadFactor <= 0.0 || loadFactor > 1.0)
            ex::raise<ex::RuntimeError>("invalid load factor: %f", loadFactor);
        reserve(sz);
    }

    /// Return true if the hash table is empty.
    bool empty() const { return nItems == 0; }

    /// Get the number of items in the hash table.
    size_t size() const { return nItems; }

    /// Get heap memory usage for this object.  Does not include
    /// sizeof this object, irrespective of whether it is allocated on
    /// the stack or the heap.
    size_t heapSize() const {
        return sizeof(value_t) * table.capacity();
    }

    /// Get the total memory footprint of the hash table.
    size_t memUsage() const {
        return sizeof(my_t) + heapSize(); 
    }

    /// Get the number of items that can be held without having to
    /// grow the hash table.
    size_t capacity() const { return loadThreshold; }

    /// Get current load factor (ratio of items to hash table size).
    double currentLoad() const
    {
        if(table.empty())
            return 0.0;
        else
            return double(nItems) / table.size();
    }

    /// Get maximum load factor (ratio of items to hash table size).
    double maxLoad() const { return loadFactor; }

    /// Reserve enough space to hold \c sz items without having to
    /// grow the hash table.
    void reserve(size_t sz)
    {
        if(sz > loadThreshold)
        {
            size_t cap = size_t(ceil(sz / loadFactor));
            grow(cap);
        }
    }

    /// Insert an item into the hash table, resizing if necessary to
    /// maintain the load factor established at construction.
    void insert(item_t const & x)
    {
        if(++nItems > loadThreshold)
        {
            grow(std::max(table.size() << 1,
                          size_t(ceil(nItems / loadFactor))));
        }
        assert(nItems <= table.size());

        hashkey_t k = hashItem(x);
        value_t v(k,x);
        std::swap(*probeAvailable(k), v);
    }

    /// Insert an item into the hash table only if it isn't already
    /// there.  Resizes if necessary to maintain the load factor
    /// established at construction.
    void insertUnique(item_t const & x)
    {
        if(!contains(x))
            insert(x);
    }

    /// Insert an item into the hash table, overwriting any existing
    /// entry with the same key.  Resizes if necessary to maintain the
    /// load factor established at construction.
    void insertOverwrite(item_t const & x)
    {
        hashkey_t k = hashItem(x);
        iter_t p = probeEqual(k, x);
        if(p != table.end())
        {
            value_t v(k, x);
            std::swap(*p, v);
        }
        else
            insert(x);
    }

    /// Erase an item from the hash table, if it exists.
    template <class KeyType>
    void erase(KeyType const & x)
    {
        iter_t p = probeEqual(hashItem(x), x);
        if(p != table.end())
        {
            value_t v(deletedKey(), item_t());
            std::swap(*p, v);
            --nItems;
        }
    }

    /// Check to see if the hash table contains the given item.
    template <class KeyType>
    bool contains(KeyType const & x) const
    {
        return probeEqual(hashItem(x), x) != table.end();
    }

    /// Get the maximum probe length in the hash table.
    size_t maxProbe() const
    {
        if(!nItems)
            return 1;

        size_t maxProbeLen = 1;
        size_t cap = table.size();
        for(size_t pos = 0; pos != cap; ++pos)
        {
            hashkey_t k = table[pos].first;
            if(k != emptyKey() && k != deletedKey())
            {
                size_t probeStart = size_t(k) % cap;
                size_t probeLen;
                if(probeStart > pos)
                    probeLen = 1 + cap - probeStart + pos;
                else
                    probeLen = 1 + pos - probeStart;
                if(probeLen > maxProbeLen)
                    maxProbeLen = probeLen;
            }
        }
        return maxProbeLen;
    }

    /// Get the average probe length in the hash table.
    double averageProbe() const
    {
        if(!nItems)
            return 1.0;

        size_t totalProbeLen = 0;
        size_t cap = table.size();
        for(size_t pos = 0; pos != cap; ++pos)
        {
            hashkey_t k = table[pos].first;
            if(k != emptyKey() && k != deletedKey())
            {
                size_t probeStart = size_t(k) % cap;
                if(probeStart > pos)
                    totalProbeLen += 1 + cap - probeStart + pos;
                else
                    totalProbeLen += 1 + pos - probeStart;
            }
        }
        return totalProbeLen / double(nItems);
    }

    /// Optimize hash table by reclaiming deleted slots and rehashing
    /// remaining entries.
    void rehash()
    {
        size_t cap = table.size();
        iter_t p0 = table.begin();
        iter_t p1 = table.end();

        bool somethingChanged;
        do
        {
            somethingChanged = false;
            for(iter_t p = p0; p != p1; ++p)
            {
                if(p->first == deletedKey())
                {
                    p->first = emptyKey();
                    somethingChanged = true;
                }
                else if(p->first != emptyKey())
                {
                    iter_t pp = p0 + size_t(p->first) % cap;
                    while(pp != p && pp->first != emptyKey() &&
                          pp->first != deletedKey())
                    {
                        if(++pp == p1)
                            pp = p0;
                    }
                    if(pp != p)
                    {
                        std::swap(*p, *pp);
                        p->first = emptyKey();
                        somethingChanged = true;
                    }
                }
            }
        } while(somethingChanged);
    }

    /// Rebuild the current hash table using the given load factor as
    /// a target.
    void rebuildToLoad(double targetLoad)
    {
        using namespace ex;
        if(targetLoad <= 0.0 || targetLoad > 1.0)
            raise<ValueError>("invalid target load: %f", targetLoad);
        
        size_t newCap = size_t(nItems / targetLoad);
        grow(newCap);
    }

    /// Clear out all entries in hash table.
    void clear()
    {
        std::fill(table.begin(), table.end(), value_t(emptyKey(), item_t()));
        nItems = 0;
    }

    /// Swap contents of two hash tables.
    void swap(my_t & o)
    {
        std::swap(nItems, o.nItems);
        std::swap(loadThreshold, o.loadThreshold);
        std::swap(loadFactor, o.loadFactor);
        table.swap(o.table);
        std::swap(hash, o.hash);
        std::swap(equals, o.equals);
    }

    class const_iterator
    {
        citer_t it;
    public:
        const_iterator() {}
        const_iterator(citer_t const & it) : it(it) {}
        
        bool operator==(const_iterator const & o) const {
            return it == o.it;
        }

        bool operator!=(const_iterator const & o) const {
            return !(*this == o);
        }

        const_iterator const & operator++() {
            ++it;
            return *this;
        }
        const_iterator const & operator--() {
            --it;
            return *this;
        }

        item_t const * operator->() const { return &it->second; }
        item_t const & operator*() const { return it->second; }

        bool deleted() const { return it->first == deletedKey(); }
        bool empty() const { return it->first == emptyKey(); }
        bool exists() const { return !empty() && !deleted(); }
    };

    /// Find something
    template <class KeyType>
    const_iterator find(KeyType const & x) const
    {
        return probeEqual(hashItem(x), x);
    }
    
    const_iterator begin() const { return table.begin(); }
    const_iterator end() const { return table.end(); }
};


#endif // WARP_HASHTABLE_H
