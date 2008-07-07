//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-06-11
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

#include <warp/memfs.h>
#include <warp/file.h>
#include <warp/shared.h>
#include <ex/exception.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>

using namespace warp;
using namespace ex;
using namespace std;
using boost::shared_ptr;
using boost::static_pointer_cast;

//----------------------------------------------------------------------------
// utils
//----------------------------------------------------------------------------
namespace
{
    inline double now()
    {
        struct timeval t;
        gettimeofday(&t, 0);
        return t.tv_sec + t.tv_usec * 1e-6;
    }

    inline string normpath(string const & uri)
    {
        // Get path component
        string p(fs::path(uri));

        // Trim trailing slash, but not root slash
        size_t sz = p.size();
        while(sz > 1 && p[sz-1] == '/')
            --sz;
        if(sz != p.size())
            p.erase(sz);

        return p;
    }
}


//----------------------------------------------------------------------------
// FS singleton
//----------------------------------------------------------------------------
shared_ptr<MemoryFilesystem> const & MemoryFilesystem::get()
{
    static shared_ptr<MemoryFilesystem> fs(new MemoryFilesystem);
    return fs;
}


//----------------------------------------------------------------------------
// MemoryFilesystem::Item
//----------------------------------------------------------------------------
class MemoryFilesystem::Item
{

public:
    double mtime;
    double atime;
    double ctime;

public:
    Item() { mtime = atime = ctime = now(); }
    virtual ~Item() {}
        
    void touchRead() { atime = now(); }
    void touchWrite() { mtime = atime = now(); }

    virtual size_t size() const = 0;
    virtual bool isFile() const { return false; }
    virtual bool isDirectory() const { return false; }
};


//----------------------------------------------------------------------------
// FileItem
//----------------------------------------------------------------------------
class MemoryFilesystem::FileItem : public MemoryFilesystem::Item
{
    vector<char> buf;

public:
    size_t write(size_t pos, void const * data, size_t len)
    {
        size_t end = pos + len;
        if(end < pos)
            raise<IOError>("integer overflow");

        if(end > buf.size())
            buf.resize(end);

        memcpy(&buf[pos], data, len);
        touchWrite();

        return len;
    }

    size_t read(size_t pos, void * data, size_t len)
    {
        if(pos >= buf.size())
            return 0;

        size_t end = pos + len;
        if(end < pos)
            raise<IOError>("integer overflow");

        if(end > buf.size())
            end = buf.size();
            
        len = end - pos;
        memcpy(data, &buf[pos], len);
        touchRead();

        return len;
    }

    void truncate() { buf.clear(); }
    size_t size() const { return buf.size(); }
    bool isFile() const { return true; }
};


//----------------------------------------------------------------------------
// DirItem
//----------------------------------------------------------------------------
class MemoryFilesystem::DirItem : public MemoryFilesystem::Item
{
public:
    size_t size() const { return 1; }
    bool isDirectory() const { return true; }
};


//----------------------------------------------------------------------------
// MemoryFilesystem::FileHandle
//----------------------------------------------------------------------------
class MemoryFilesystem::FileHandle : public warp::File
{
    shared_ptr<FileItem> item;
    string uri;
    off_t pos;
    int mode;

public:
    FileHandle(string const & uri, int mode) :
        item(MemoryFilesystem::get()->openFileItem(uri, mode)),
        uri(uri),
        pos(0),
        mode(mode)
    {
    }

    size_t read(void * dst, size_t elemSz, size_t nElem)
    {
        if(!item)
            raise<IOError>("read on closed file");

        if((mode & O_ACCMODE) == O_WRONLY)
            raise<IOError>("read of write-only file");

        if(!elemSz)
            raise<ValueError>("zero element size");

        size_t nBytes = item->read(pos, dst, elemSz * nElem);
        size_t nItems = nBytes / elemSz;
        pos += nItems * elemSz;
        return nItems;
    }

    size_t write(void const * src, size_t elemSz, size_t nElem)
    {
        if(!item)
            raise<IOError>("write on closed file");

        if((mode & O_ACCMODE) == O_RDONLY)
            raise<IOError>("write of read-only file");

        if(!elemSz)
            raise<ValueError>("zero element size");

        if(mode & O_APPEND)
            pos = item->size();

        size_t nBytes = item->write(pos, src, elemSz * nElem);
        size_t nItems = nBytes / elemSz;
        pos += nItems * elemSz;
        return nItems;
    }

    void close()
    {
        item.reset();
    }

    off_t tell() const
    {
        if(!item)
            raise<IOError>("write on closed file");

        return pos;
    }

    void seek(off_t offset, int whence)
    {
        if(!item)
            raise<IOError>("write on closed file");

        off_t target;
        switch(whence)
        {
            case SEEK_SET:
                target = offset;
                break;

            case SEEK_CUR:
                target = offset + pos;
                break;

            case SEEK_END:
                target = offset + item->size();
                break;

            default:
                raise<IOError>("unknown seek mode: %d", whence);
        }

        if(target < 0)
            raise<IOError>("seek before beginning of file: position=%d, file=%s",
                           target, getName());

        pos = target;
    }

    string getName() const
    {
        return uri;
    }
};


//----------------------------------------------------------------------------
// MemoryFilesystem
//----------------------------------------------------------------------------
MemoryFilesystem::~MemoryFilesystem()
{
}

MemoryFilesystem::itemptr_t MemoryFilesystem::find(string const & uri) const
{
    map_t::const_iterator it = fsMap.find(normpath(uri));
    if(it != fsMap.end())
        return it->second;
    else
        return itemptr_t();
}

MemoryFilesystem::fileptr_t
MemoryFilesystem::openFileItem(string const & uri, int mode)
{
    bool tryCreate = false;
    bool doTrunc = false;
    switch(mode & O_ACCMODE)
    {
        case O_WRONLY:
        case O_RDWR:
            if(mode & O_CREAT)
                tryCreate = true;
            if(mode & O_TRUNC)
                doTrunc = true;
            break;
    }

    fileptr_t fp;
    if(tryCreate)
    {
        itemptr_t & ip = fsMap[normpath(uri)];

        if(!ip)
            ip.reset(new FileItem);
        else if(mode & O_EXCL)
            raise<IOError>("file already exists: %s", uri);
        else if(!ip->isFile())
            raise<IOError>("path exists but is not a file: %s", uri);

        fp = static_pointer_cast<FileItem>(ip);
    }
    else
    {
        itemptr_t ip = find(uri);
        if(!ip)
            raise<IOError>("file does not exist: %s", uri);
        else if(!ip->isFile())
            raise<IOError>("path exists but is not a file: %s", uri);

        fp = static_pointer_cast<FileItem>(ip);
    }

    if(doTrunc)
        fp->truncate();
    
    return fp;
}

bool MemoryFilesystem::mkdir(string const & uri)
{
    itemptr_t & ip = fsMap[normpath(uri)];
    if(!ip)
    {
        ip.reset(new DirItem);
        return true;
    }
    else if(ip->isDirectory())
        return false;
    else
        raise<IOError>("path already exists: %s", uri);
}

bool MemoryFilesystem::remove(string const & uri)
{
    map_t::iterator it = fsMap.find(normpath(uri));
    if(it == fsMap.end())
        return false;

    if(!it->second->isFile())
        raise<NotImplementedError>("MemoryFilesystem can only remove files");
    
    fsMap.erase(it);
    return true;
}

void MemoryFilesystem::rename(string const & sourceUri,
                              string const & targetUri,
                              bool overwrite)
{
    // Must rename on the same filesystem
    if(Filesystem::get(targetUri).get() != this)
        raise<RuntimeError>("cannot rename across filesystems: '%s' to '%s'",
                            sourceUri, targetUri);

    // Get the source entry
    map_t::iterator src = fsMap.find(normpath(sourceUri));
    if(src == fsMap.end())
        raise<IOError>("cannot rename '%s' to '%s': source does not exist",
                       sourceUri, targetUri);

    // Get the target entry
    map_t::iterator dst = fsMap.find(normpath(targetUri));
    if(dst != fsMap.end())
    {
        // Target already exists - bail unless we want to overwrite
        if(!overwrite)
            raise<IOError>("cannot rename '%s' to '%s': target exists",
                           sourceUri, targetUri);

        // Can never overwrite a directory
        if(dst->second->isDirectory())
            raise<IOError>("cannot rename '%s' to '%s': cannot overwrite a directory",
                           sourceUri, targetUri);

        // Can't overwrite a file with a directory
        if(src->second->isDirectory())
            raise<IOError>("cannot rename '%s' to '%s': cannot overwrite a file with a directory",
                           sourceUri, targetUri);

        // Don't move file to itself
        if(src == dst)
            raise<IOError>("cannot rename '%s' to '%s': they are the same file",
                           sourceUri, targetUri);

        // Overwrite target
        dst->second = src->second;
    }
    else
    {
        // Target doesn't exist -- insert it
        fsMap[normpath(targetUri)] = src->second;
    }

    // Remove source
    fsMap.erase(src);
}

size_t MemoryFilesystem::filesize(string const & uri)
{
    itemptr_t it(find(uri));

    if(!it || !it->isFile())
        raise<IOError>("filesize on non-file: %s", uri);
    
    return it->size();
}

bool MemoryFilesystem::exists(string const & uri)
{
    return find(uri);
}

bool MemoryFilesystem::isDirectory(string const & uri)
{
    itemptr_t it(find(uri));
    return it && it->isDirectory();
}

bool MemoryFilesystem::isFile(string const & uri)
{
    itemptr_t it(find(uri));
    return it && it->isFile();
}

bool MemoryFilesystem::isEmpty(string const & uri)
{
    itemptr_t it(find(uri));
    if(!it)
        raise<IOError>("path does not exist: %s", uri);
    return it->size() == 0;
}

double MemoryFilesystem::modificationTime(string const & uri)
{
    itemptr_t it(find(uri));
    if(!it)
        raise<IOError>("path does not exist: %s", uri);
    return it->mtime;
}

double MemoryFilesystem::accessTime(string const & uri)
{
    itemptr_t it(find(uri));
    if(!it)
        raise<IOError>("path does not exist: %s", uri);
    return it->atime;
}

double MemoryFilesystem::creationTime(string const & uri)
{
    itemptr_t it(find(uri));
    if(!it)
        raise<IOError>("path does not exist: %s", uri);
    return it->ctime;
}

void MemoryFilesystem::clear()
{
    fsMap.clear();
}


//----------------------------------------------------------------------------
// Registration
//----------------------------------------------------------------------------
namespace
{
    FilePtr openFile(string const & uri, int mode)
    {
        return newShared<MemoryFilesystem::FileHandle>(uri, mode);
    }

    FsPtr getFs(string const & uri)
    {
        return MemoryFilesystem::get();
    }
}

#include <warp/init.h>
WARP_DEFINE_INIT(warp_memfs)
{
    Filesystem::registerScheme("memfs", &getFs);
    File::registerScheme("memfs", &openFile);
}
