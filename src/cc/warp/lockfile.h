//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/lockfile.h#1 $
//
// Created 2006/12/30
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_LOCKFILE_H
#define WARP_LOCKFILE_H

#include <string>

namespace warp
{
    /// Attempt to exclusively create the local file \c fn.  If \c cmt
    /// is non-empty, fill the lock file with the given comment
    /// string.  The lock file may be released by deleting it.
    /// @return True if the file was created, false if it already existed.
    /// @throws IOError if some other error condition occurs.
    bool attemptLock(std::string const & fn, std::string const & cmt);

    /// Exclusively create the local lock file \c fn.  If \c cmt is
    /// non-empty, fill the lock file with the given comment string
    /// when it is created.  If the attempt to create the lock file
    /// fails, this function will retry up to \c maxRetries times.  If
    /// \c maxRetries is negative, it will never give up.  Between
    /// each attempt, it will sleep for \c sleepTime seconds.  If \c
    /// lockTimeout is positive and a previously existing lock file is
    /// older than \c lockTimeout seconds, the lock will be forced
    /// (deleted), and the function will sleep for \c suspendTime
    /// seconds before attempting to acquire the lock.  Typically \c
    /// suspendTime is greater than \c sleepTime to prevent multiple
    /// processes from forcing each other's lock.  If \c verbose is
    /// true, status updates will be written to stderr.  This function
    /// will not return until the lock succeeds.  It will throw an
    /// exception if it fails for some reason.  The lock file may be
    /// released by deleting it.
    /// @throws RuntimeError if \c maxRetries is exceeded
    /// @throws IOError if there is a problem with the lock file
    void lockFile(std::string const & fn, std::string const & cmt,
                  int maxRetries, double lockTimeout, double sleepTime,
                  double suspendTime, bool verbose);
}

#endif // WARP_LOCKFILE_H
