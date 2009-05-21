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
#include <warp/dir.h>
#include <warp/shared.h>
#include <warp/timestamp.h>
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
    inline string normpath(string const & uri)
    {
        // Get path component
        string p = fs::resolve("/", fs::path(uri));

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
    Timestamp mtime;
    Timestamp atime;
    Timestamp ctime;

public:
    Item() { mtime = atime = ctime = Timestamp::now(); }
    virtual ~Item() {}

    void touchRead() { atime = Timestamp::now(); }
    void touchWrite() { mtime = atime = Timestamp::now(); }

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
    map_t entries;

public:
    size_t size() const { return entries.size(); }
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
// MemoryFilesystem::DirHandle
//----------------------------------------------------------------------------
class MemoryFilesystem::DirHandle : public Directory
{
public:
    explicit DirHandle(std::string const & uri) :
        base(uri), idx(0)
    {
        MemoryFilesystem::itemptr_t p = MemoryFilesystem::get()->find(uri);
        if(!p)
            raise<IOError>("directory does not exist: %s", uri);
        if(!p->isDirectory())
            raise<IOError>("not a directory: %s", uri);

        MemoryFilesystem::dirptr_t d =
            static_pointer_cast<MemoryFilesystem::DirItem>(p);
        for(map_t::const_iterator i = d->entries.begin();
            i != d->entries.end(); ++i)
        {
            files.push_back(i->first);
        }
    }

    virtual void close()
    {
        files.clear();
    }

    virtual bool read(std::string & dst)
    {
        if(idx >= files.size())
            return false;

        dst = files[idx];
        ++idx;
        return true;
    }

    virtual std::string getPath() const
    {
        return base;
    }

public:
    std::string base;
    size_t idx;
    std::vector<string> files;
};


//----------------------------------------------------------------------------
// MemoryFilesystem
//----------------------------------------------------------------------------
MemoryFilesystem::MemoryFilesystem() :
    root(new DirItem)
{
}

MemoryFilesystem::~MemoryFilesystem()
{
}

MemoryFilesystem::itemptr_t & MemoryFilesystem::insert(string const & uri)
{
    std::string path = normpath(uri);
    if(path == "/")
        return root;

    dirptr_t p = findParent(path);
    if(!p)
        raise<IOError>("parent dir doesn't exist: %s [ %s ]", uri, path);

    return p->entries[fs::basename(path)];
}

MemoryFilesystem::itemptr_t MemoryFilesystem::find(string const & uri) const
{
    std::string path = normpath(uri);
    if(path == "/")
        return root;

    dirptr_t p = findParent(path);
    if(!p)
        return itemptr_t();

    map_t::const_iterator it = p->entries.find(fs::basename(path));
    if(it != p->entries.end())
        return it->second;
    else
        return itemptr_t();
}

//#include <iostream>
//#include <warp/strutil.h>

MemoryFilesystem::dirptr_t MemoryFilesystem::findParent(string const & uri) const
{
    //cout << "findParent: " << reprString(uri) << endl;

    string path = normpath(uri);
    if(path == "/")
    {
        //cout << "  root  -->  NULL" << endl;
        return dirptr_t();
    }

    std::string parent = normpath(fs::resolve(uri, ".."));
    //cout << "  parent : " << reprString(parent) << endl;

    char const * p = parent.c_str();
    char const * end = p + parent.size();
    ++p;

    dirptr_t n = static_pointer_cast<DirItem>(root);

    if(p == end)
    {
        //cout << "  root  -->  ROOT" << endl;
        return n;
    }

    for(;;)
    {
        char const * pp = std::find(p, end, '/');
        //cout << "  seg : " << reprString(p, pp) << endl;

        map_t::const_iterator i = n->entries.find(string(p, pp));
        if(i == n->entries.end())
        {
            //cout << "  not found  -->  NULL" << endl;
            return dirptr_t();
        }

        if(!i->second->isDirectory())
        {
            //cout << "  found file  -->  NULL" << endl;
            return dirptr_t();
        }

        n = static_pointer_cast<DirItem>(i->second);

        if(pp == end)
            break;

        p = pp + 1;
    }

    //cout << "  end  -->  DIR" << endl;

    return n;
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
        itemptr_t & ip = insert(uri);

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
    itemptr_t & ip = insert(uri);
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
    string path = normpath(uri);
    if(path == "/")
        raise<IOError>("cannot remove root");

    dirptr_t p = findParent(path);
    if(!p)
        return false;

    string base = fs::basename(path);
    map_t::iterator it = p->entries.find(base);
    if(it == p->entries.end())
        return false;

    if(it->second->isDirectory() && it->second->size())
        raise<IOError>("directory not empty: %s", uri);

    p->entries.erase(it);
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

    itemptr_t src = find(sourceUri);
    if(!src)
        raise<IOError>("cannot rename '%s' to '%s': source does not exist",
                       sourceUri, targetUri);

    if(src == root)
        raise<IOError>("cannot rename '%s' to '%s': source is root",
                       sourceUri, targetUri);

    itemptr_t dst = find(targetUri);
    if(dst)
    {
        // Target already exists - bail unless we want to overwrite
        if(!overwrite)
            raise<IOError>("cannot rename '%s' to '%s': target exists",
                           sourceUri, targetUri);

        // Can never overwrite a directory
        if(dst->isDirectory())
            raise<IOError>("cannot rename '%s' to '%s': cannot overwrite a directory",
                           sourceUri, targetUri);

        // Can't overwrite a file with a directory
        if(src->isDirectory())
            raise<IOError>("cannot rename '%s' to '%s': cannot overwrite a file with a directory",
                           sourceUri, targetUri);

        // Don't move file to itself
        if(src == dst)
            raise<IOError>("cannot rename '%s' to '%s': they are the same file",
                           sourceUri, targetUri);
    }

    // Overwrite target
    insert(targetUri) = src;

    // Remove source
    findParent(sourceUri)->entries.erase(fs::basename(sourceUri));
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

Timestamp MemoryFilesystem::modificationTime(string const & uri)
{
    itemptr_t it(find(uri));
    if(!it)
        raise<IOError>("path does not exist: %s", uri);
    return it->mtime;
}

Timestamp MemoryFilesystem::accessTime(string const & uri)
{
    itemptr_t it(find(uri));
    if(!it)
        raise<IOError>("path does not exist: %s", uri);
    return it->atime;
}

Timestamp MemoryFilesystem::creationTime(string const & uri)
{
    itemptr_t it(find(uri));
    if(!it)
        raise<IOError>("path does not exist: %s", uri);
    return it->ctime;
}

void MemoryFilesystem::clear()
{
    root.reset(new DirItem);
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

    DirPtr openDir(string const & uri)
    {
        return newShared<MemoryFilesystem::DirHandle>(uri);
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
    Directory::registerScheme("memfs", &openDir);
}
