//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/main/cosmix/src/cc/warp/kfs/kfsfs.h#8 $
//
// Created 2006/09/19
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
//
//----------------------------------------------------------------------------

#ifndef WARP_KFSFS_H
#define WARP_KFSFS_H

#include <kfs/KfsClient.h>
#include <warp/fs.h>
#include <string>

namespace warp {
namespace kfs {

    class KfsFilesystem;

} // namespace kfs
} // namespace warp

//----------------------------------------------------------------------------
// KfsFilesystem
//----------------------------------------------------------------------------
class warp::kfs::KfsFilesystem
    : public warp::Filesystem
{
public:
    KfsFilesystem(KFS::KfsClientPtr const & client);

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

    // Get the initialized client for a URI
    static KFS::KfsClientPtr getClient(std::string const & uri);

private:
    KFS::KfsClientPtr client;
};

#endif // WARP_KFSFS_H
