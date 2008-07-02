//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/dir.h#1 $
//
// Created 2005/12/02
//
// Copyright 2005 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WDS_DIR_H
#define WDS_DIR_H

#include <stddef.h>
#include <string>
#include <boost/shared_ptr.hpp>

namespace warp
{
    class Directory;

    typedef boost::shared_ptr<Directory> DirPtr;
    typedef DirPtr (DirOpenFunc)(std::string const & uri);
}

//----------------------------------------------------------------------------
// Directory
//----------------------------------------------------------------------------
class warp::Directory
{
public:
    virtual ~Directory() {}

    /// Close directory handle.
    virtual void close() = 0;

    /// Read next directory entry and store its name in \c dst.  A
    /// directory entry is only a single path component, not a full
    /// path to the file.  If a path is desired, use fs::resolve() to
    /// construct it, or use readPath() instead.
    /// @return True if an entry was available, false at end of
    /// directory.
    virtual bool read(std::string & dst) = 0;

    /// Read next directory entry and store its full path in \c dst.
    /// @return True if an entry was available, false at end of
    /// directory.
    virtual bool readPath(std::string & dst);

    /// Get path URI for this directory.  This path may be used as a
    /// base for fs::resolve() when constructing a path to an entry
    /// returned a call to read().  Note this typically means that the
    /// directory path will end with a slash.
    virtual std::string getPath() const = 0;


    //----------------------------------------------------------------------
    // Registration and dispatch

    /// Open a directory handle for the given URI.
    static DirPtr open(std::string const & uri);

    /// Register a directory opening handler for a given scheme.
    static void registerScheme(char const * scheme, DirOpenFunc * func);

    /// Set a default scheme for directory URIs with no scheme
    /// otherwise specified.
    static void setDefaultScheme(char const * scheme);
};



#endif // WDS_FILEIO_H
