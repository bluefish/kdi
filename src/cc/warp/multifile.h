//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-20
// 
// This file is part of the warp library.
// 
// The warp library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The warp library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef WARP_MULTIFILE_H
#define WARP_MULTIFILE_H

#include <warp/file.h>
#include <warp/generator.h>
#include <boost/utility.hpp>

namespace warp
{
    class MultiFile;
    class FlushMultiFile;
}

//----------------------------------------------------------------------------
// MultiFile
//----------------------------------------------------------------------------
/// A single File interface over a series of underlying files.
class warp::MultiFile : public warp::File, public boost::noncopyable
{
public:
    typedef Generator<FilePtr>::handle_t gen_t;

private:
    gen_t fileGenerator;
    FilePtr currentFile;

public:
    explicit MultiFile(gen_t const & fileGenerator = gen_t());
    ~MultiFile();

    /// Set file generator.
    void setGenerator(gen_t const & gen);

    /// Open next file from generator.
    /// @return True if there is a next file, false if the generator
    /// is exhausted.
    bool openNext();

    /// Close currently open file.
    virtual void close();

    /// Read from current file.  If the read fails in the current
    /// file, the read will be retried in the next file from the
    /// generator, and the next, until at least one element is read or
    /// the generator is exhausted.
    /// @see File::read
    virtual size_t read(void * dst, size_t elemSz, size_t nElem);

    /// Readline from current file.  If the read fails in the current
    /// file, the read will be retried in the next file from the
    /// generator, and the next, until at a line is read or the
    /// generator is exhausted.
    /// @see File::readline
    virtual size_t readline(char * dst, size_t sz, char delim);

    /// Write to current file.  If no file is currently open, the next
    /// file is retrieved from the generator.
    /// @see File::write
    virtual size_t write(void const * src, size_t elemSz, size_t nElem);

    /// Flush current file if open.
    /// @see File::flush
    virtual void flush();

    /// Position in current file if open, or 0 otherwise.
    /// @see File::tell
    virtual off_t tell() const;

    /// Seek is unsupported.
    /// @throw NotImplementedError
    virtual void seek(off_t offset, int whence);

    /// Get name of current file if open, or empty string otherwise.
    virtual std::string getName() const;
};


//----------------------------------------------------------------------------
// FlushMultiFile
//----------------------------------------------------------------------------
/// A MultiFile that will close its currently open file when it gets a
/// call to flush().  Useful for transparently splitting output into
/// multiple files.
class warp::FlushMultiFile : public warp::MultiFile
{
    typedef warp::MultiFile super;

public:
    explicit FlushMultiFile(gen_t const & gen = gen_t());

    /// Flush and close current file if open.  Next write will have to
    /// generate a new open file.
    /// @see File::flush
    virtual void flush();
};


#endif // WARP_MULTIFILE_H
