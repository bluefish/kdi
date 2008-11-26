//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-06-08
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
