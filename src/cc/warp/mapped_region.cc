//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/mapped_region.cc#1 $
//
// Created 2007/08/31
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "mapped_region.h"
#include "fs.h"
#include "util.h"
#include "ex/exception.h"
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

using namespace warp;
using namespace ex;
using namespace std;

MappedRegion::MappedRegion(string const & uri, int prot, int flags) :
    mem(MAP_FAILED), sz(0), fd(-1)
{
    // Open file descriptor
    string path = fs::path(uri);
    fd = ::open(path.c_str(), O_RDONLY);
    if(fd == -1)
        raise<IOError>("failed to open '%s' (path=%s): %s",
                       uri, path, getStdError());
    
    // Get file size
    struct stat st;
    if(0 != ::fstat(fd, &st))
    {
        int err = errno;
        ::close(fd);
        fd = -1;
        raise<IOError>("failed to stat '%s': %s",
                       path, getStdError(err));
    }
    sz = st.st_size;

    // Map file
    mem = ::mmap(0, sz, prot, flags, fd, 0);
    if(mem == MAP_FAILED)
    {
        int err = errno;
        ::close(fd);
        fd = -1;
        raise<IOError>("failed to mmap %d bytes from '%s': %s",
                       sz, path, getStdError(err));
    }
}

MappedRegion::MappedRegion(string const & uri, int prot, int flags,
                           size_t sz, off_t offset) :
    mem(MAP_FAILED), sz(sz), fd(-1)
{
    // Open file descriptor
    string path = fs::path(uri);
    fd = ::open(path.c_str(), O_RDONLY);
    if(fd == -1)
        raise<IOError>("failed to open '%s' (path=%s): %s",
                       uri, path, getStdError());
    
    // Map file
    mem = ::mmap(0, sz, prot, flags, fd, offset);
    if(mem == MAP_FAILED)
    {
        int err = errno;
        ::close(fd);
        fd = -1;
        raise<IOError>("failed to mmap %d bytes from '%s' at offset %d: %s",
                       sz, path, offset, getStdError(err));
    }
}

MappedRegion::MappedRegion(int prot, int flags, size_t sz) :
    mem(MAP_FAILED), sz(sz), fd(-1)
{
    // Require anonymous mapping bit
    flags |= MAP_ANONYMOUS;

    // Map region
    mem = ::mmap(0, sz, prot, flags, -1, 0);
    if(mem == MAP_FAILED)
        raise<IOError>("failed to mmap anonymous region of size %d: %s",
                       sz, getStdError());
}

MappedRegion::~MappedRegion()
{
    // Unmap region
    if(mem != MAP_FAILED)
        ::munmap(mem, sz);

    // Close file descriptor
    if(fd != -1)
        ::close(fd);
}

void MappedRegion::prefault() const
{
    prefault(0, sz);
}

void MappedRegion::prefault(size_t off, size_t len) const
{
    size_t const PAGE_SZ = getpagesize();
    
    size_t base = alignDown(off, PAGE_SZ);
    size_t end = alignUp(off + len, PAGE_SZ);

    volatile char c = 42;
    for(size_t p = base; p < end; p += PAGE_SZ)
    {
        c += *getAs<char>(p);
    }
}
