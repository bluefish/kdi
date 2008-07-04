//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-12-30
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

#include <warp/lockfile.h>
#include <warp/fs.h>
#include <ex/exception.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

using namespace warp;
using namespace ex;
using namespace std;

bool warp::attemptLock(string const & fn, string const & cmt)
{
    // Try to open file exclusively
    int fd = ::open(fn.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0444);
    if(fd == -1)
    {
        if(errno == EEXIST)
            return false;
        else
            raise<IOError>("could not create lockfile '%s': %s",
                           fn, getStdError());
    }
            
    // Try to write lock comment
    if(!cmt.empty())
    {
        ssize_t sz = ::write(fd, cmt.c_str(), cmt.length());
        if(sz != (ssize_t)cmt.length())
        {
            int err = errno;
            ::close(fd);
            ::unlink(fn.c_str());
            raise<IOError>("could not write lockfile '%s': %s",
                           fn, getStdError(err));
        }
    }

    // Close lock file
    if(0 != ::close(fd))
    {
        int err = errno;
        ::unlink(fn.c_str());
        raise<IOError>("could not close lockfile '%s': %s",
                       fn, getStdError(err));
    }
    return true;
}


void warp::lockFile(string const & fn, string const & cmt, int maxRetries,
                    double lockTimeout, double sleepTime, double suspendTime,
                    bool verbose)
{
    if(verbose)
        cerr << "Locking: " << fn << endl;

    // Keep trying lock until we get it or run out of attempts
    int retry = 0;
    while(!attemptLock(fn, cmt))
    {
        // Failed to get it -- sleep a bit
        double zzzTime = sleepTime;
        if(verbose)
            cerr << "Attempt " << (retry+1) << ": lock failed" << endl;

        // If the lock file is older than than lockTimeout, force the
        // lock (delete it)
        if(lockTimeout > 0)
        {
            double now = time(0);
            double mtime = fs::modificationTime(fn);
            if(now - mtime >= lockTimeout)
            {
                if(verbose)
                    cerr << "Forcing lock: " << fn << endl;

                // Delete the lock, sleep for suspendTime instead of
                // sleepTime
                ::unlink(fn.c_str());
                zzzTime = suspendTime;
            }
        }

        // Check to see if we've made too many attempts
        if(maxRetries >= 0 && retry >= maxRetries)
            raise<RuntimeError>("too many attempts on lockfile: %s", fn);
        retry += 1;

        // Sleep and make another attempt
        if(verbose)
            cerr << "Sleeping " << zzzTime << " seconds" << endl;
        ::usleep((unsigned int)(zzzTime * 1e6));
    }
}
