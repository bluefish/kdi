//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-09-19
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

#include "posixfs.h"
#include "uri.h"
#include "ex/exception.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <dirent.h>
#include <errno.h>

using namespace warp;
using namespace ex;
using std::string;


//----------------------------------------------------------------------------
// PosixFilesystem
//----------------------------------------------------------------------------
PosixFilesystem::PosixFilesystem()
{
}

bool PosixFilesystem::mkdir(string const & uri)
{
    string path = fs::path(uri);

    int r = ::mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
    if(r != 0)
    {
        int mkdirErr = errno;
        if(mkdirErr == EEXIST)
        {
            // Already exists -- okay if it's a directory
            struct stat st;
            if(0 == ::stat(path.c_str(), &st) && S_ISDIR(st.st_mode))
                return false;
        }
        raise<IOError>("mkdir failed for '%s': %s", uri,
                       getStdError(mkdirErr));
    }
    return true;
}

bool PosixFilesystem::remove(string const & uri)
{
    string path = fs::path(uri);
    
    int r = ::remove(path.c_str());
    if(r != 0 && errno != ENOENT)
        raise<IOError>("remove failed for '%s': %s", uri, getStdError());
    return (r == 0);
}

void PosixFilesystem::rename(string const & sourceUri,
                             string const & targetUri,
                             bool overwrite)
{
    // Make sure we're on the same filesystem
    if(Filesystem::get(targetUri).get() != this)
        raise<RuntimeError>("cannot rename across filesystems: '%s' to '%s'",
                            sourceUri, targetUri);

    // Get paths
    string sourcePath = fs::path(sourceUri);
    string targetPath = fs::path(targetUri);

    // Stat source
    struct stat st;
    int r = ::stat(sourcePath.c_str(), &st);
    if(r != 0)
        raise<IOError>("rename '%s' to '%s' failed: %s", sourceUri, targetUri,
                       getStdError());

    if(!overwrite && S_ISREG(st.st_mode))
    {
        // Try to rename without overwrite
        
        // Make link to new file
        r = ::link(sourcePath.c_str(), targetPath.c_str());
        if(r != 0)
            raise<IOError>("link '%s' to '%s' failed: %s", sourceUri,
                           targetUri, getStdError());
        // Unlink old file
        r = ::unlink(sourcePath.c_str());
        if(r != 0)
            raise<IOError>("unlink '%s' failed: %s", sourceUri, getStdError());
    }
    else
    {
        // Just rename
        r = ::rename(sourcePath.c_str(), targetPath.c_str());
        if(r != 0)
            raise<IOError>("rename '%s' to '%s' failed: %s", sourceUri,
                           targetUri, getStdError());
    }
}

size_t PosixFilesystem::filesize(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = ::stat(path.c_str(), &st);
    if(r != 0)
        raise<IOError>("filesize failed for '%s': %s", uri, getStdError());

    if(!S_ISREG(st.st_mode))
        raise<IOError>("filesize called on non-file '%s'", uri);
    
    return st.st_size;
}

bool PosixFilesystem::exists(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = ::stat(path.c_str(), &st);
    if(r != 0 && errno != ENOENT)
        raise<IOError>("exists failed for '%s': %s", uri, getStdError());

    return (r == 0);
}

bool PosixFilesystem::isDirectory(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = ::stat(path.c_str(), &st);
    if(r != 0 && errno != ENOENT)
        raise<IOError>("isDirectory failed for '%s': %s", uri, getStdError());

    return (r == 0 && S_ISDIR(st.st_mode));
}

bool PosixFilesystem::isFile(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = ::stat(path.c_str(), &st);
    if(r != 0 && errno != ENOENT)
        raise<IOError>("isFile failed for '%s': %s", uri, getStdError());

    return (r == 0 && S_ISREG(st.st_mode));
}

bool PosixFilesystem::isEmpty(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = ::stat(path.c_str(), &st);
    if(r != 0)
        raise<IOError>("isEmpty failed for '%s': %s", uri, getStdError());

    if(S_ISREG(st.st_mode))
    {
        return st.st_size == 0;
    }
    else if(S_ISDIR(st.st_mode))
    {
        // Check for empty directory
        DIR * dir = opendir(path.c_str());
        if(!dir)
            raise<IOError>("could not open directory '%s': %s", uri,
                           getStdError());
        
        bool dirEmpty = true;
        for(struct dirent * d = readdir(dir); d; d = readdir(dir))
        {
            char const * n = d->d_name;
            if(n[0] == '.' && (n[1] == '\0' || (n[1] == '.' && n[2] == '\0')))
            {
                // d_name is either . or ..
            }
            else
            {
                dirEmpty = false;
                break;
            }
        }

        closedir(dir);

        return dirEmpty;
    }
    else
    {
        raise<IOError>("isEmpty called on non-file, non-directory '%s'", uri);
    }

    return (r == 0 && S_ISREG(st.st_mode));
}

double PosixFilesystem::modificationTime(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = ::stat(path.c_str(), &st);
    if(r != 0)
        raise<IOError>("modificationTime failed for '%s': %s", uri,
                       getStdError());

    return double(st.st_mtime) + double(st.st_mtim.tv_nsec) * 1e-9;
}

double PosixFilesystem::accessTime(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = ::stat(path.c_str(), &st);
    if(r != 0)
        raise<IOError>("accessTime failed for '%s': %s", uri, getStdError());

    return double(st.st_atime) + double(st.st_atim.tv_nsec) * 1e-9;
}

double PosixFilesystem::creationTime(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = ::stat(path.c_str(), &st);
    if(r != 0)
        raise<IOError>("creationTime failed for '%s': %s", uri, getStdError());

    return double(st.st_ctime) + double(st.st_ctim.tv_nsec) * 1e-9;
}

string PosixFilesystem::getCwd()
{
    char buf[PATH_MAX];
    char * r = getcwd(buf, sizeof(buf));
    if(!r)
        raise<IOError>("getCwd failed: %s", getStdError());
    return string(buf);
}

FsPtr PosixFilesystem::get()
{
    static FsPtr fs(new PosixFilesystem());
    return fs;
}


//----------------------------------------------------------------------------
// Registration
//----------------------------------------------------------------------------
namespace 
{
    FsPtr getPosixFs(string const & uri)
    {
        return PosixFilesystem::get();
    }
}

#include "init.h"
WARP_DEFINE_INIT(warp_posixfs)
{
    Filesystem::registerScheme("file",  &getPosixFs);
    Filesystem::registerScheme("async", &getPosixFs);
    Filesystem::registerScheme("mmap", &getPosixFs);
    Filesystem::setDefaultScheme("file");
}
