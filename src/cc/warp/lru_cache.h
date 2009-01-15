//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-06-11
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

#ifndef WARP_LRU_CACHE_H
#define WARP_LRU_CACHE_H

#include <warp/circular.h>
#include <warp/objectpool.h>
#include <boost/noncopyable.hpp>
#include <map>

namespace warp {

    template <class K, class V>
    class LruCache;

} // namespace warp

//----------------------------------------------------------------------------
// LruCache
//----------------------------------------------------------------------------
template <class K, class V>
class warp::LruCache
    : private boost::noncopyable
{
    typedef K key_t;
    typedef V value_t;

    struct Item;
    typedef std::map<key_t, Item *> map_t;

    struct Item : public Circular<Item>
    {
        value_t value;
        size_t refCount;
        typename map_t::iterator indexIt;
        bool removed;

        Item() :
            value(), refCount(0), removed(false) {}

        /// Given the address of a value_t contained in an Item,
        /// return the address of the containing Item.  Note, the
        /// caller must ensure that the given value_t is actually in
        /// an Item.  If it isn't this will crash and burn.
        static Item * castFromValue(value_t * ptr)
        {
            // This looks sketchy but it's ok (I think!).
            Item * item = 0;
            ptrdiff_t off = reinterpret_cast<ptrdiff_t>(&item->value);
            item = reinterpret_cast<Item *>(
                reinterpret_cast<char *>(ptr) - off
                );
            return item;
        }
    };

    typedef warp::ObjectPool<Item> pool_t;
    typedef warp::CircularList<Item> lru_t;

    map_t index;
    pool_t pool;
    lru_t lru;

    size_t maxItems;

    /// Flush least recently used items with no outstanding
    /// references until cache is under budget or all items have
    /// references.
    void flushToSize(size_t targetSize)
    {
        typename lru_t::iterator i = lru.begin();
        while(i != lru.end() && index.size() > targetSize)
        {
            if(i->refCount == 0)
                removeItem(&*i++);
            else
                ++i;
        }
    }

    void removeItem(Item * item)
    {
        // Take the item out of the index
        index.erase(item->indexIt);
        
        // Release item
        pool.release(item);
    }

public:
    /// Make an LruCache holding a certain number of items.  The cache
    /// can temporarily grow larger than the maxSize if clients are
    /// slow in releasing cached objects.
    explicit LruCache(size_t maxItems) :
        maxItems(maxItems)
    {
    }

    /// Return true iff the cache contains an element for the given
    /// key.  This will not change the LRU ordering or insert new
    /// items.  It just indicates that a call to get() for the same
    /// item will return a previously existing item.
    bool contains(key_t const & key) const
    {
        return index.find(key) != index.end();
    }

    /// Return the number of items in the cache.
    size_t size() const
    {
        return index.size();
    }

    /// Get an item out of the cache.  If the key is already in the
    /// cache, the associated item will be returned.  Otherwise a new
    /// item will default-constructed, associated with the key, and
    /// returned.  The returned item will be pinned in the cache until
    /// a balanced call to release().  An item may be held by multiple
    /// callers simultaneously.  The item will remain pinned until the
    /// last call to release().
    value_t * get(key_t const & key)
    {
        // Find the key in the index -- create a new item if necessary
        typename map_t::iterator i = index.find(key);
        if(i == index.end())
        {
            // Release unused items until cache is under budget
            if(index.size() >= maxItems)
                flushToSize(maxItems-1);

            // Create a new item
            i = index.insert(std::make_pair(key, pool.get())).first;
            i->second->indexIt = i;
        }

        // Increment reference count
        ++i->second->refCount;
        i->second->removed = false;

        // Move new item to most recently used spot in cache
        lru.moveToBack(i->second);

        // Return value
        return &i->second->value;
    }

    /// Remove a key from the cache.  If the item for the key has
    /// already been flushed from the cache, do nothing.  If the key
    /// refers to an item still in the cache, remove it as soon all
    /// references to it are released.  A subsequent get() request
    /// will clear the removal flag.
    void remove(key_t const & key)
    {
        typename map_t::iterator i = index.find(key);
        if(i != index.end())
        {
            // Remove the item if there are no references.  Otherwise
            // set the removal flag.
            if(i->second->refCount == 0)
                removeItem(i->second);
            else
                i->second->removed = true;
        }
    }

    /// Release the item.  Once all holders of the item have released
    /// it, it may be removed from the cache.  If the optional second
    /// parameter is true, the item will be removed from the cache as
    /// soon as all references to it are released.  A subsequent get()
    /// request will clear the removal flag.
    void release(value_t * value, bool remove=false)
    {
        // Recover the item pointer from the value
        Item * item = Item::castFromValue(value);

        // Set removal flag
        if(remove)
            item->removed = true;

        // Decrement reference count and maybe remove item
        if(--item->refCount == 0 && (item->removed || index.size() > maxItems))
            removeItem(item);
    }

    /// Remove all items from the cache.  This is equivalent to
    /// calling remove() on every key in the cache.  Like remove(),
    /// the items will not actually be removed until all references to
    /// it have been released.  Subsequent get() calls will clear the
    /// removal flag.
    void removeAll()
    {
        flushToSize(0);
    }
};

#endif // WARP_LRU_CACHE_H
