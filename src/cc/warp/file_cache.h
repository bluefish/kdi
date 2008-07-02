//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/file_cache.h $
//
// Created 2008/06/08
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <warp/file.h>
#include <warp/synchronized.h>
#include <boost/noncopyable.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>

#ifndef WARP_FILE_CACHE_H
#define WARP_FILE_CACHE_H

namespace warp {

    class FileCache;
    typedef boost::shared_ptr<FileCache> FileCachePtr;

} // namespace warp


//----------------------------------------------------------------------------
// FileCache
//----------------------------------------------------------------------------
class warp::FileCache
    : public boost::enable_shared_from_this<FileCache>,
      private boost::noncopyable
{
    enum { BLOCK_SIZE = 256 << 10 };

    class FilePool;
    class BlockCache;
    class Block;
    class CachedFile;

    boost::shared_ptr< Synchronized<FilePool> > syncFilePool;
    boost::shared_ptr< Synchronized<BlockCache> > syncBlockCache;

private:
    FileCache(size_t cacheSize, size_t maxHandles);

public:
    static FileCachePtr make(size_t cacheSize, size_t maxHandles);
    FilePtr open(std::string const & uri, int mode=O_RDONLY);
};

#endif // WARP_FILE_CACHE_H
