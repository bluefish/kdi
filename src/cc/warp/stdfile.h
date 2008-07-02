//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/stdfile.h#1 $
//
// Created 2006/01/11
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_STDFILE_H
#define WARP_STDFILE_H

#include "file.h"
#include <stdio.h>
#include <boost/utility.hpp>

namespace warp
{
    class StdFile;
}


//----------------------------------------------------------------------------
// StdFile
//----------------------------------------------------------------------------
/// A File wrapper over the C stdio library.
class warp::StdFile : public File, public boost::noncopyable
{
    FILE * fp;
    std::string fn;

public:
    StdFile(std::string const & uri, char const * mode);
    StdFile(int fd, std::string const & name, char const * mode);
    ~StdFile();

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


#endif // WARP_STDFILE_H
