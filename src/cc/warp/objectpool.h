//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-03-23
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

#ifndef WARP_OBJECTPOOL_H
#define WARP_OBJECTPOOL_H

#include "util.h"
#include "ex/exception.h"
#include <vector>
#include <algorithm>
#include <iostream>
#include <boost/utility.hpp>

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
        ~PooledItem() { destroy(); }

        object_t * get()
        {
            object_t * obj = reinterpret_cast<object_t *>(data);
            if(!initialized)
            {
                new (obj) T();
                initialized = true;
            }
            return obj;
        }

        void destroy()
        {
            if(initialized)
            {
                get()->~T();
                initialized = false;
            }
        }
    };

    typedef std::vector<PooledItem *> blockvec_t;
    typedef typename blockvec_t::const_iterator blockiter_t;

    blockvec_t blocks;
    PooledItem * freeList;
    size_t blockSize;
    bool reconstructObjects;

public:
    enum { DEFAULT_BLOCK_SIZE = 16 };

public:
    explicit ObjectPool(size_t blockSize = DEFAULT_BLOCK_SIZE,
                        bool reconstructObjects = true) : 
        freeList(0),
        blockSize(blockSize),
        reconstructObjects(reconstructObjects)
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

    /// Get an available object from the pool.  If no object is
    /// currently available, a new block will be allocated and one
    /// will be returned from there.
    object_t * get()
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

        return item->get();
    }

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
        if(reconstructObjects)
            item->destroy();

        // Return object to free list
        item->next = freeList;
        freeList = item;
    }
};


#endif // WARP_OBJECTPOOL_H
