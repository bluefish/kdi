//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/memfs.h#1 $
//
// Created 2007/06/11
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_MEMFS_H
#define WARP_MEMFS_H

#include "fs.h"
#include <map>
#include <string>
#include <boost/shared_ptr.hpp>

namespace warp
{
    /// Simple filesystem stored in memory.  This filesystem is NOT
    /// thread safe!
    class MemoryFilesystem;
}


//----------------------------------------------------------------------------
// MemoryFilesystem
//----------------------------------------------------------------------------
class warp::MemoryFilesystem : public warp::Filesystem
{
public:
    class Item;
    class FileItem;
    class DirItem;
    class FileHandle;
    typedef boost::shared_ptr<Item> itemptr_t;
    typedef boost::shared_ptr<FileItem> fileptr_t;
    typedef std::map<std::string, itemptr_t> map_t;

private:
    map_t fsMap;

    itemptr_t find(std::string const & uri) const;
    fileptr_t openFileItem(std::string const & uri, int mode);

    // Singleton, no copy
    MemoryFilesystem() {}
    MemoryFilesystem(MemoryFilesystem const & o);
    MemoryFilesystem const & operator=(MemoryFilesystem const & o);

public:
    virtual ~MemoryFilesystem();

    virtual bool mkdir(std::string const & uri);
    virtual bool remove(std::string const & uri);
    virtual void rename(std::string const & sourceUri,
                        std::string const & targetUri,
                        bool overwrite);
    virtual size_t filesize(std::string const & uri);
    virtual bool exists(std::string const & uri);
    virtual bool isDirectory(std::string const & uri);
    virtual bool isFile(std::string const & uri);
    virtual bool isEmpty(std::string const & uri);
    virtual double modificationTime(std::string const & uri);
    virtual double accessTime(std::string const & uri);
    virtual double creationTime(std::string const & uri);

    /// Remove everything in the filesystem.    
    void clear();

    /// Get singleton instance
    static boost::shared_ptr<MemoryFilesystem> const & get();
};

#endif // WARP_MEMFS_H
