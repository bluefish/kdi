//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/server/BlockCache.h $
//
// Created 2009/02/27
//
// Copyright 2009 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_SERVER_BLOCKCACHE_H
#define KDI_SERVER_BLOCKCACHE_H

#include <stddef.h>

namespace kdi {
namespace server {

    class BlockCache;

    // Forward declaration
    class Fragment;
    class FragmentBlock;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// BlockCache
//----------------------------------------------------------------------------
class kdi::server::BlockCache
{
public:
    /// Get the FragmentBlock at blockAddr for the given Fragment.
    /// The returned block must eventually be released with a balanced
    /// call to releaseBlock().
    virtual FragmentBlock const * getBlock(
        Fragment const * fragment, size_t blockAddr) = 0;

    /// Release a FragmentBlock previously acquired by getBlock().
    virtual void releaseBlock(FragmentBlock const * block) = 0;

protected:
    ~BlockCache() {}
};


#endif // KDI_SERVER_BLOCKCACHE_H
