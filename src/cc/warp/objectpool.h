//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-03-23
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

#ifndef WARP_OBJECTPOOL_H
#define WARP_OBJECTPOOL_H

#include <warp/util.h>
#include <ex/exception.h>
#include <vector>
#include <algorithm>
#include <iostream>
#include <boost/utility.hpp>
#include <boost/preprocessor/repetition.hpp>

#ifndef WARP_OBJECT_POOL_MAX_PARAMS
#  define WARP_OBJECT_POOL_MAX_PARAMS 5
#endif


namespace warp
{
    template <class T>
    class ObjectPool;
}

//----------------------------------------------------------------------------
// ObjectPool
//----------------------------------------------------------------------------
template <class T>
class warp::ObjectPool : private boost::noncopyable
{
    typedef T object_t;
    enum { OBJECT_SIZE = sizeof(T) };

    struct PooledItem
    {
        char data[OBJECT_SIZE];
        PooledItem * next;
        bool initialized;

        PooledItem() : initialized(false) {}
        ~PooledItem() { if(initialized) cast()->~T(); }
        object_t * cast() { return reinterpret_cast<object_t *>(data); };
    };

    typedef std::vector<PooledItem *> blockvec_t;
    typedef typename blockvec_t::const_iterator blockiter_t;

    blockvec_t blocks;
    PooledItem * freeList;
    size_t blockSize;
    bool destroyOnRelease;

public:
    enum { DEFAULT_BLOCK_SIZE = 16 };

public:
    explicit ObjectPool(size_t blockSize = DEFAULT_BLOCK_SIZE,
                        bool destroyOnRelease = true) :
        freeList(0),
        blockSize(blockSize),
        destroyOnRelease(destroyOnRelease)
    {
    }

    ~ObjectPool()
    {
#ifndef NDEBUG
        // Check to see if all objects have been released before
        // deleting them
        for(blockiter_t bi = blocks.begin(); bi != blocks.end(); ++bi)
        {
            PooledItem * block = *bi;
            for(size_t i = 0; i < blockSize; ++i)
            {
                if(block[i].next == reinterpret_cast<PooledItem *>(this))
                {
                    std::cerr << "ObjectPool: deleting pooled object at 0x"
                              << (void const *)block[i].data
                              << " before it has been released"
                              << std::endl;
                }
            }
        }
#endif

        // Delete objects
        std::for_each(blocks.begin(), blocks.end(), delete_arr());
    }


private:
    /// Get an item from the pool.  If no item is currently available,
    /// a new block will be allocated and one will be returned from
    /// there.
    PooledItem * getItem()
    {
        if(!freeList)
        {
            blocks.push_back(new PooledItem[blockSize]);
            freeList = blocks.back();
            for(size_t i = 1; i < blockSize; ++i)
                freeList[i-1].next = &freeList[i];
            freeList[blockSize-1].next = 0;
        }

        PooledItem * item = freeList;
        freeList = item->next;

        // Use the next pointer to track owner pool for allocated
        // objects
        item->next = reinterpret_cast<PooledItem *>(this);

        return item;
    }

public:
    /// Get an available object from the pool.  If no object is
    /// currently available, a new block will be allocated and one
    /// will be returned from there.  If destroyOnRelease is false,
    /// the item returned may have been previously used.  Otherwise it
    /// will be default constructed.
    object_t * get()
    {
        PooledItem * item = getItem();
        T * obj = item->cast();
        if(!item->initialized)
        {
            new (obj) T();
            item->initialized = true;
        }
        return obj;
    }

    /// Create an object from the pool.  The memory from the object
    /// may be reused from the pool, but the object will be
    /// re-initialized before being returned.
    object_t * create()
    {
        PooledItem * item = getItem();
        T * obj = item->cast();
        if(item->initialized)
            obj->~T();
        new (obj) T();
        item->initialized = true;
        return obj;
    }

    // create() function for 1 to WARP_OBJECT_POOL_MAX_PARAMS
    // arguments
    #undef WARP_OBJECT_POOL_CREATE
    #define WARP_OBJECT_POOL_CREATE(z,n,unused)         \
    template <BOOST_PP_ENUM_PARAMS(n, class A)>         \
    object_t * create(                                  \
        BOOST_PP_ENUM_BINARY_PARAMS(n, A, const & a)    \
        )                                               \
    {                                                   \
        PooledItem * item = getItem();                  \
        T * obj = item->cast();                         \
        if(item->initialized)                           \
            obj->~T();                                  \
        new (obj) T(BOOST_PP_ENUM_PARAMS(n,a));         \
        item->initialized = true;                       \
        return obj;                                     \
    }
    BOOST_PP_REPEAT_FROM_TO(1, BOOST_PP_ADD(WARP_OBJECT_POOL_MAX_PARAMS, 1),
                            WARP_OBJECT_POOL_CREATE, ~)
    #undef WARP_OBJECT_POOL_CREATE

    /// Release an object and make it available in the pool again.
    /// The object must have previously been obtained from this pool
    /// using a call to get().
    void release(object_t * obj)
    {
        // Get containing PooledItem pointer
        PooledItem * item = reinterpret_cast<PooledItem *>(obj);

        // Make sure object came from this pool
        if(item->next != reinterpret_cast<PooledItem *>(this))
        {
            using namespace ex;
            raise<RuntimeError>("ObjectPool::release() called on invalid "
                                "object: pool=%p obj=%p owner=%p",
                                this, obj, item->next);
        }

        // Destroy object now if we want to reconstruct it later
        if(destroyOnRelease)
        {
            item->cast()->~T();
            item->initialized = false;
        }

        // Return object to free list
        item->next = freeList;
        freeList = item;
    }
};


#endif // WARP_OBJECTPOOL_H
