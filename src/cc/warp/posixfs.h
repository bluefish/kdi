//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/posixfs.h#1 $
//
// Created 2006/09/19
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_POSIXFS_H
#define WARP_POSIXFS_H

#include "fs.h"

namespace warp
{
    class PosixFilesystem;
}

//----------------------------------------------------------------------------
// PosixFilesystem
//----------------------------------------------------------------------------
class warp::PosixFilesystem : public warp::Filesystem
{
private:
    // Do not construct this object.  Use get().
    PosixFilesystem();

public:
    // Filesystem interface
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

    // Extra functions

    /// Get current working directory.  The working directory is
    /// returned as a simple path -- the scheme, authority, query, and
    /// fragment sections of the URI are undefined.
    static std::string getCwd();

    // Get implementation
    static FsPtr get();
};

#endif // WARP_POSIXFS_H
