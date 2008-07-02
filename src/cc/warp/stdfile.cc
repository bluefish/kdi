//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/stdfile.cc#1 $
//
// Created 2006/01/11
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "stdfile.h"
#include "uri.h"
#include "fs.h"
#include "ex/exception.h"
#include <string.h>

using namespace warp;
using namespace ex;
using namespace std;


//----------------------------------------------------------------------------
// StdFile
//----------------------------------------------------------------------------
StdFile::StdFile(string const & uri, char const * mode) :
    fp(0), fn(uri)
{
    fp = fopen(fs::path(uri).c_str(), mode);
    if(!fp)
        raise<IOError>("failed to open '%s': %s", fn, getStdError());
}

StdFile::StdFile(int fd, string const & name, char const * mode) :
    fp(0), fn(name)
{
    fp = fdopen(fd, mode);
    if(!fp)
        raise<IOError>("failed to open '%s' (fd=%s): %s",
                       fn, fd, getStdError());
}

StdFile::~StdFile()
{
    close();
}

size_t StdFile::read(void * dst, size_t elemSz, size_t nElem)
{
    if(!fp)
        raise<IOError>("read() from closed file: %s", fn);

    return fread(dst, elemSz, nElem, fp);
}

size_t StdFile::readline(char * dst, size_t sz, char delim)
{
    if(!fp)
        raise<IOError>("readline() from closed file: %s", fn);

    // Read up to EOF, delim, or sz - 1 bytes
    size_t len = 0;
    while(len + 1 < sz)
    {
        int c = getc(fp);
        if(c == EOF)
            break;
        dst[len++] = (char)c;
        if(c == delim)
            break;
    }

    // Null terminate if we were given at least 1 byte (sz != 0)
    if(len < sz)
        dst[len] = '\0';

    return len;
}

size_t StdFile::write(void const * src, size_t elemSz, size_t nElem)
{
    if(!fp)
        raise<IOError>("write() to closed file: %s", fn);

    return fwrite(src, elemSz, nElem, fp);
}

void StdFile::flush()
{
    if(!fp)
        raise<IOError>("flush() on closed file: %s", fn);

    if(fflush(fp) != 0)
        raise<IOError>("fflush failed for '%s': %s", fn, getStdError());
}

void StdFile::close()
{
    if(fp)
    {
        if(fclose(fp) != 0)
            raise<IOError>("fclose failed for '%s': %s", fn, getStdError());
        
        fp = 0;
    }
}

off_t StdFile::tell() const
{
    if(!fp)
        raise<IOError>("tell() on closed file: %s", fn);

    off_t pos = ftell(fp);
    if(pos < 0)
        raise<IOError>("ftell failed for '%s': %s", fn, getStdError());
    return pos;
}

void StdFile::seek(off_t offset, int whence)
{
    if(!fp)
        raise<IOError>("seek() on closed file: %s", fn);

    if(fseek(fp, offset, whence) != 0)
        raise<IOError>("fseek failed for '%s': %s", fn, getStdError());
}

std::string StdFile::getName() const
{
    return fn;
}


//----------------------------------------------------------------------------
// Registration
//----------------------------------------------------------------------------
namespace
{
    FilePtr openStd(string const & uri, int flags)
    {
        int const STD_MASK = O_ACCMODE | O_CREAT | O_TRUNC | O_APPEND;

        char const * mode;
        switch(flags & STD_MASK)
        {
            case O_RDONLY:
                mode = "r";
                break;

            case O_WRONLY:
            case O_RDWR:
                mode = "r+";
                break;

            case O_WRONLY | O_CREAT | O_TRUNC:
                mode = "w";
                break;

            case O_RDWR | O_CREAT | O_TRUNC:
                mode = "w+";
                break;

            case O_WRONLY | O_CREAT | O_APPEND:
                mode = "a";
                break;

            case O_RDWR | O_CREAT | O_APPEND:
                mode = "a+";
                break;

            default:
                raise<NotImplementedError>("unsupported open flags for '%s': 0x%x",
                                           uri, flags);
        }
        
        FilePtr r;
        if(fs::path(uri) == "-")
        {
            // Special file
            switch(flags & O_ACCMODE)
            {
                case O_WRONLY:
                    r.reset(new StdFile(STDOUT_FILENO, "<stdout>", mode));
                    break;
                    
                case O_RDONLY:
                    r.reset(new StdFile(STDIN_FILENO, "<stdin>", mode));
                    break;

                default:
                    raise<IOError>("cannot open '-' for reading and writing");
            }
        }
        else if(flags & ~STD_MASK)
        {
            // Open includes flags not supported by fopen -- use unix open
            int fd = open(fs::path(uri).c_str(), flags, 0666);
            if(fd == -1)
                raise<IOError>("failed to open '%s': %s", uri, getStdError());
            r.reset(new StdFile(fd, uri, mode));
        }
        else
        {
            // Use normal fopen
            r.reset(new StdFile(uri, mode));
        }

        return r;
    }
}

#include "init.h"
WARP_DEFINE_INIT(warp_stdfile)
{
    File::registerScheme("file", &openStd);
    File::registerScheme("pipe",  &openStd);
    File::setDefaultScheme("file");
}
