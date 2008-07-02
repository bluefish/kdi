//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/mmapfile.h#1 $
//
// Created 2006/04/05
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_MMAPFILE_H
#define WARP_MMAPFILE_H

#include "file.h"
#include <boost/utility.hpp>
#include <string>

namespace warp
{
    class MMapFile;
}

//----------------------------------------------------------------------------
// MMapFile
//----------------------------------------------------------------------------
/// A File interface wrapping a memory-mapped file.
/// \todo Add write support.
class warp::MMapFile : public File, public boost::noncopyable
{
    int fd;
    void * ptr;
    size_t length;
    off_t pos;
    std::string uri;

    void * addr(off_t p) const
    {
        return reinterpret_cast<void *>
            (reinterpret_cast<uintptr_t>(ptr) + p);
    }

public:
    explicit MMapFile(std::string const & uri, bool readOnly=true);
    ~MMapFile();

    // File interface
    size_t read(void * dst, size_t elemSz, size_t nElem);
    size_t readline(char * dst, size_t sz, char delim);
    size_t write(void const * src, size_t elemSz, size_t nElem);
    void flush();
    void close();
    off_t tell() const;
    void seek(off_t offset, int whence);
    std::string getName() const;
};


#endif // WARP_MMAPFILE_H
