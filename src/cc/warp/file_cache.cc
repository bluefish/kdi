//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-06-10
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

#include <warp/file_cache.h>
#include <warp/lru_cache.h>
#include <warp/util.h>
#include <warp/uri.h>
#include <warp/fs.h>
#include <ex/exception.h>
#include <list>
#include <string>
#include <map>

using namespace warp;
using namespace ex;

//----------------------------------------------------------------------------
// FileCache::FilePool
//----------------------------------------------------------------------------
class FileCache::FilePool
    : private boost::noncopyable
{
public:
    struct SharedFile
    {
        FilePtr fp;
        boost::mutex mutex;
    };

private:
    typedef warp::LruCache<std::string, SharedFile> lru_t;
    lru_t lru;

public:
    explicit FilePool(size_t maxSize) : lru(maxSize) {}

    SharedFile * get(std::string const & uri)
    {
        SharedFile * x = lru.get(uri);
        if(!x->fp)
            x->fp = File::input(uri);
        return x;
    }

    void release(SharedFile * x)
    {
        lru.release(x);
    }

    void close(std::string const & uri)
    {
        lru.remove(uri);
    }
};


//----------------------------------------------------------------------------
// FileCache::Block
//----------------------------------------------------------------------------
class FileCache::Block
{
    size_t base;
    int32_t len;
    char data[BLOCK_SIZE];

public:
    Block() : base(0), len(-1) {}

    void setBase(size_t b) { base = b; }

    void fillFrom(FilePtr const & fp)
    {
        fp->seek(base);
        len = fp->read(data, BLOCK_SIZE);
    }

    bool filled() const { return len >= 0; }

    bool contains(size_t pos) const
    {
        return pos >= base && pos < base + len;
    }

    ssize_t read(void * dst, size_t pos, size_t readLen)
    {
        if(!contains(pos))
            return 0;

        if(pos + readLen > base + len)
            readLen = base + len - pos;

        memcpy(dst, &data[pos-base], readLen);
        return readLen;
    }
};


//----------------------------------------------------------------------------
// FileCache::BlockCache
//----------------------------------------------------------------------------
class FileCache::BlockCache
    : private boost::noncopyable
{
    typedef std::pair<std::string, size_t> key_t;
    typedef warp::LruCache<key_t, Block> lru_t;

    lru_t lru;

public:
    explicit BlockCache(size_t maxBlocks) :
        lru(maxBlocks) {}

    Block * get(std::string const & uri, size_t base)
    {
        Block * b = lru.get(key_t(uri, base));
        b->setBase(base);
        return b;
    }

    void release(Block * b)
    {
        lru.release(b);
    }
};


//----------------------------------------------------------------------------
// FileCache::CachedFile
//----------------------------------------------------------------------------
class FileCache::CachedFile
    : public warp::File
{
    FileCachePtr cache;
    std::string uri;
    size_t filesize;

    Block * block;
    size_t pos;

    void releaseBlock()
    {
        if(block)
        {
            LockedPtr<BlockCache> blockCache(*cache->syncBlockCache);
            blockCache->release(block);
            block = 0;
        }
    }

    // Get the block for the current position
    bool getBlock()
    {
        if(pos >= filesize)
        {
            // The new location is past EOF.  If there's a current
            // block, release it.
            releaseBlock();
            return false;
        }

        // Release the current block, if any, and get a new one out of
        // the cache.
        {
            LockedPtr<BlockCache> blockCache(*cache->syncBlockCache);
            if(block)
            {
                blockCache->release(block);
                block = 0;
            }
            block = blockCache->get(uri, alignDown(pos, BLOCK_SIZE));
        }

        // Make sure block is filled
        if(!block->filled())
            fillBlock(block);

        return true;
    }

    void fillBlock(Block * b)
    {
        // Get the shared file from the cache (make sure it is
        // released at the end of this function)
        FilePool::SharedFile * sf =
            LockedPtr<FilePool>(*cache->syncFilePool)->get(uri);

        try {
            // Get exclusive access to the file
            boost::mutex::scoped_lock fileLock(sf->mutex);
            
            // Fill the block if it hasn't been already
            if(!b->filled())
                b->fillFrom(sf->fp);
        }
        catch(...) {
            // Release the shared file if something goes wrong
            LockedPtr<FilePool>(*cache->syncFilePool)->release(sf);
            throw;
        }

        // Release the shared file
        LockedPtr<FilePool>(*cache->syncFilePool)->release(sf);
    }
    
public:
    CachedFile(FileCachePtr const & cache, std::string const & uri) :
        cache(cache),
        uri(uri),
        filesize(fs::filesize(uri)),
        block(0),
        pos(0)
    {
        EX_CHECK_NULL(cache);
    }
    
    ~CachedFile()
    {
        close();
    }

    virtual size_t read(void * dst, size_t elemSz, size_t nElem)
    {
        if(!cache)
            raise<IOError>("read() on closed file: %s", getName());
        if(!elemSz)
            raise<ValueError>("need positive element size");

        // Trivial out
        if(!nElem)
            return 0;

        // Make sure we have a block ready
        if(!block && !getBlock())
            return 0;

        // Read until the buffer is full or we run out of blocks
        char * buf = static_cast<char *>(dst);
        char * end = buf + (elemSz * nElem);
        for(;;)
        {
            // Read and advance pointers.  The read is safe -- if the
            // block doesn't contain this position, it will return a 0
            // byte read.
            size_t n = block->read(buf, pos, end-buf);
            buf += n;
            pos += n;

            // Are we done?
            if(buf == end)
            {
                // We've read the full amount
                return nElem;
            }

            // There's more to read, get the next block
            if(!getBlock())
            {
                // We're at EOF with bytes left remaining.  If we were
                // in mid-element, un-read that data.
                size_t bytesRead = (elemSz * nElem) - (end - buf);
                size_t nElemRead = bytesRead / elemSz;
                size_t partialBytes = bytesRead - nElemRead * elemSz;
                pos -= partialBytes;
                return nElemRead;
            }
        }
    }

    virtual size_t write(void const * src, size_t elemSz, size_t nElem)
    {
        EX_UNIMPLEMENTED_FUNCTION;
    }

    virtual void flush()
    {
        if(!cache)
            raise<IOError>("flush() on closed file");
    }

    virtual void close()
    {
        if(cache)
        {
            releaseBlock();
            LockedPtr<FilePool>(*cache->syncFilePool)->close(uri);
            cache.reset();
        }
    }

    virtual off_t tell() const
    {
        if(!cache)
            raise<IOError>("tell() on closed file");

        return pos;
    }

    virtual void seek(off_t offset, int whence)
    {
        if(!cache)
            raise<IOError>("seek() on closed file");

        switch(whence)
        {
            case SEEK_SET: break;
            case SEEK_CUR: offset += pos; break;
            case SEEK_END: offset += filesize; break;
            default:
                raise<ValueError>("invalid seek mode: %d", whence);
        }

        if(offset < 0)
            raise<IOError>("seek before beginning of file: position=%d, file=%s",
                           offset, getName());

        pos = offset;
    }

    virtual std::string getName() const
    {
        return uriPushScheme(uri, "cache");
    }
};


//----------------------------------------------------------------------------
// FileCache
//----------------------------------------------------------------------------
FileCache::FileCache(size_t cacheSize, size_t maxHandles) :
    syncFilePool(
        new Synchronized<FilePool>(maxHandles)
        ),
    syncBlockCache(
        new Synchronized<BlockCache>(
            (cacheSize + BLOCK_SIZE - 1) / BLOCK_SIZE
            )
        )
{
}

FileCachePtr FileCache::make(size_t cacheSize, size_t maxHandles)
{
    FileCachePtr p(new FileCache(cacheSize, maxHandles));
    return p;
}

FilePtr FileCache::open(std::string const & uri, int mode)
{
    if((mode & O_ACCMODE) != O_RDONLY)
        raise<ValueError>("FileCache only supports read-only files");

    FilePtr p(new CachedFile(shared_from_this(), uri));
    return p;
}


//----------------------------------------------------------------------------
// Init
//----------------------------------------------------------------------------
#include <warp/init.h>
#include <warp/dir.h>
#include <stdlib.h>

namespace
{
    class AutoCache
    {
        FileCachePtr cache;
        
        AutoCache()
        {
            size_t nHandles = 64;
            size_t cacheSize = 128 << 20;

            if(char * s = getenv("WARP_FILE_CACHE_SIZE"))
                cacheSize = parseSize(s);
            if(char * s = getenv("WARP_FILE_CACHE_HANDLES"))
                nHandles = parseSize(s);

            cache = FileCache::make(cacheSize, nHandles);
        }

    public:
        static FileCachePtr const & get()
        {
            static AutoCache x;
            return x.cache;
        }
    };

    FsPtr getFs(std::string const & uri)
    {
        return Filesystem::get(uriPopScheme(uri));
    }

    DirPtr openDir(std::string const & uri)
    {
        return Directory::open(uriPopScheme(uri));
    }

    FilePtr openFile(std::string const & uri, int mode)
    {
        return AutoCache::get()->open(uriPopScheme(uri), mode);
    }
}

WARP_DEFINE_INIT(warp_file_cache)
{
    File::registerScheme("cache", &openFile);
    Directory::registerScheme("cache", &openDir);
    Filesystem::registerScheme("cache", &getFs);
}
