//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-05-03
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

#ifndef WARP_BUFFEREDFILE_H
#define WARP_BUFFEREDFILE_H

#include <warp/file.h>
#include <warp/buffer.h>
#include <string>
#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>

namespace warp
{
    class BufferedFile;

    typedef boost::shared_ptr<BufferedFile> BufferedFilePtr;
}


//----------------------------------------------------------------------------
// BufferedFile
//----------------------------------------------------------------------------
class warp::BufferedFile : public File, public boost::noncopyable
{
    enum { DEFAULT_BUFFER_SIZE = 128 << 10 };
    enum Mode { MODE_READ, MODE_WRITE };

    Buffer buf;
    FilePtr fp;
    off_t pos;
    Mode mode;

    inline size_t fillBuffer();
    inline void spillBuffer();
    inline void requireMode(Mode m);

public:
    explicit BufferedFile(FilePtr const & fp,
                          size_t bufSz = DEFAULT_BUFFER_SIZE);
    virtual ~BufferedFile();
    virtual size_t read(void * dst, size_t elemSz, size_t nElem);
    virtual size_t readline(char * dst, size_t sz, char delim);
    virtual size_t write(void const * src, size_t elemSz, size_t nElem);
    virtual void flush();
    virtual void close();
    virtual off_t tell() const;
    virtual void seek(off_t offset, int whence);
    virtual std::string getName() const;

    using File::read;
    using File::write;
    using File::seek;
    
    /// Skip forward from current position until the given byte
    /// sequence is found.  Read at most \c maxRead bytes while
    /// searching (default is effectively unlimited).  If a match is
    /// found, the file position is left at the start of the matching
    /// byte sequence.  Otherwise, it is left after the last byte that
    /// could have started a match.  Note that this search gets less
    /// efficient as the pattern length approaches the buffer size.
    /// @return true if pattern is found
    bool skipTo(char const * pattern, size_t patternLen,
                size_t maxRead = size_t(-1));

    // Get a character at the given position
    char get(off_t pos);
};


#endif // WARP_BUFFEREDFILE_H
