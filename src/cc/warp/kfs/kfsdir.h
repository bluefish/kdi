//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/main/cosmix/src/cc/warp/kfs/kfsdir.h#7 $
//
// Created 2006/09/21
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
//
//----------------------------------------------------------------------------

#ifndef WARP_KFSDIR_H
#define WARP_KFSDIR_H

#include <kfs/KfsClient.h>
#include <warp/dir.h>
#include <vector>
#include <string>

namespace warp {
namespace kfs {

    class KfsDirectory;

} // namespace kfs
} // namespace warp

//----------------------------------------------------------------------------
// KfsDirectory
//----------------------------------------------------------------------------
class warp::kfs::KfsDirectory
    : public warp::Directory
{
public:
    KfsDirectory(KFS::KfsClientPtr const & client, std::string const & uri);
    virtual ~KfsDirectory();

    // Directory interface
    virtual void close();
    virtual bool read(std::string & dst);
    virtual std::string getPath() const;

private:
    std::string uri;
    std::vector<std::string> entries;
    std::vector<std::string>::size_type idx;
};


#endif // WARP_KFSDIR_H
