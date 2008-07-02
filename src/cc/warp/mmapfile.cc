//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/mmapfile.cc#1 $
//
// Created 2006/04/05
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "mmapfile.h"
#include "uri.h"
#include "fs.h"
#include "ex/exception.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>

using namespace warp;
using namespace ex;

//----------------------------------------------------------------------------
// MMapFile
//----------------------------------------------------------------------------
MMapFile::MMapFile(std::string const & uri, bool readOnly) :
    fd(-1), ptr(MAP_FAILED), length(0), pos(0), uri(uri)
{
    if(!readOnly)
        raise<NotImplementedError>("MMapFile does not support writable files");

    fd = ::open(fs::path(uri).c_str(), O_RDONLY | O_LARGEFILE);
    if(fd == -1)
        raise<IOError>("failed to open '%s': %s", uri, getStdError());

    off_t endPos = lseek(fd, 0, SEEK_END);
    if(endPos < 0)
        raise<IOError>("lseek returned %ld: %s", endPos, getStdError());
    length = endPos;
    ptr = mmap(0, length, PROT_READ, MAP_SHARED, fd, 0);
    if(ptr == MAP_FAILED)
    {
        ptr = 0;
        raise<IOError>("mmap failed: %s", getStdError());
    }

    off_t offset;
    Uri u(wrap(uri));
    if(u.fragment && parseInt(offset, u.fragment))
    {
        if(offset < 0)
            seek(offset, SEEK_END);
        else
            seek(offset, SEEK_SET);
    }
}

MMapFile::~MMapFile()
{
    if(ptr != MAP_FAILED)
        munmap(ptr, length);
    if(fd != -1)
        ::close(fd);
}

size_t MMapFile::read(void * dst, size_t elemSz, size_t nElem)
{
    size_t sz = elemSz * nElem;
    if(pos + sz > length)
    {
        nElem = (length - pos) / elemSz;
        sz = elemSz * nElem;
    }

    memcpy(dst, addr(pos), sz);
    pos += sz;
    return nElem;
}

size_t MMapFile::readline(char * dst, size_t sz, char delim)
{
    if(!sz)
        return 0;
    --sz;
  
    if(pos + sz > length)
        sz = length - pos;

    char const * p = reinterpret_cast<char const *>(addr(pos));
    size_t len = 0;
    while(len < sz)
    {
        char c = *p++;
        dst[len++] = c;
        if(c == delim)
            break;
    }

    pos += len;
    
    dst[len] = '\0';
    return len;
}

size_t MMapFile::write(void const * src, size_t elemSz, size_t nElem)
{
    raise<NotImplementedError>("MMapFile does not support writable files");
}

void MMapFile::flush()
{
    raise<NotImplementedError>("MMapFile does not support writable files");
}

void MMapFile::close()
{
    if(ptr != MAP_FAILED)
    {
        int err = munmap(ptr, length);
        ptr = MAP_FAILED;
        if(err)
            raise<IOError>("munmap returned %d: %s", err, getStdError());
    }
    if(fd != -1)
    {
        if(int err = ::close(fd))
            raise<IOError>("close returned %d: %s", err, getStdError());
    }
}

off_t MMapFile::tell() const
{
    return pos;
}

void MMapFile::seek(off_t offset, int whence)
{
    off_t p;
    switch(whence)
    {
        case SEEK_SET:
            p = offset;
            break;
            
        case SEEK_CUR:
            p = offset + pos;
            break;

        case SEEK_END:
            p = offset + length;
            break;

        default:
            raise<IOError>("unknown seek mode: %d", whence);
    }

    if(p < 0)
        raise<IOError>("seek to negative position: %ld", p);
    else if(p <= (off_t)length)
        pos = p;
    else
        pos = length;
}

std::string MMapFile::getName() const
{
    return uri;
}


//----------------------------------------------------------------------------
// Registration
//----------------------------------------------------------------------------
namespace
{
    FilePtr openMMap(std::string const & uri, int flags)
    {
        bool readOnly;
        switch(flags)
        {
            case O_RDONLY:
                readOnly = true;
                break;

            default:
                raise<NotImplementedError>("unsupported open flags for '%s': 0x%x",
                                           uri, flags);
        }
        
        FilePtr r(new MMapFile(uri, readOnly));
        return r;
    }
}

#include "init.h"
WARP_DEFINE_INIT(warp_mmapfile)
{
    File::registerScheme("mmap", &openMMap);
}
