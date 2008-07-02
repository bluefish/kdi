//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/posixdir.h#1 $
//
// Created 2006/09/20
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_POSIXDIR_H
#define WARP_POSIXDIR_H

#include "dir.h"
#include <sys/types.h>
#include <dirent.h>
#include <boost/utility.hpp>

namespace warp
{
    class PosixDirectory;
}

//----------------------------------------------------------------------------
// PosixDirectory
//----------------------------------------------------------------------------
class warp::PosixDirectory : public warp::Directory, public boost::noncopyable
{
private:
    DIR * dir;
    std::string name;

public:
    PosixDirectory(std::string const & uri);
    virtual ~PosixDirectory();

    // Directory interface
    virtual void close();
    virtual bool read(std::string & dst);
    virtual std::string getPath() const;
};

#endif // WARP_POSIXDIR_H
