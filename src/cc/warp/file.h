//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2005 Josh Taylor (Kosmix Corporation)
// Created 2005-12-02
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

#ifndef WDS_FILE_H
#define WDS_FILE_H

#include <stdint.h>
#include <stddef.h>
#include <string>
#include <fcntl.h>
#include <boost/shared_ptr.hpp>

namespace warp
{
    class File;

    /// Shared smart pointer to a File.
    typedef boost::shared_ptr<File> FilePtr;


    /// Open function prototype
    typedef FilePtr (FileOpenFunc)(std::string const & uri, int mode);
}


//----------------------------------------------------------------------------
// File
//----------------------------------------------------------------------------
/// Base class for objects that support file-like operations.
/// @todo Add truncate() method?
class warp::File
{
public:
    virtual ~File() {}

    /// Read up to \c nElem elements, each of size \c elemSz, and
    /// store them in the buffer pointed to by \c dst.
    /// @return Number of elements (not bytes) successfully read.
    virtual size_t read(void * dst, size_t elemSz, size_t nElem) = 0;

    /// Read up to \c sz - 1 characters and store them in \c dst.
    /// Reading stops after encountering \c delim or EOF.  If \c delim
    /// is read, it is included in \c dst.  A null character is stored
    /// after the last character in the \c dst buffer, and the length
    /// of the buffer is returned.  Since the buffer may contain
    /// nulls, it's better to trust the return value than strlen().
    /// @return Number of bytes stored in \c dst.  Zero indicates EOF.
    virtual size_t readline(char * dst, size_t sz, char delim);

    /// Write \c nElem elements, each of size \c elemSz, taken from the
    /// buffer pointed to by \c src.
    /// @return Number of elements (not bytes) successfully written.
    virtual size_t write(void const * src, size_t elemSz, size_t nElem) = 0;

    /// Flush output to lower level handler.  Clears any internal
    /// buffering, but doesn't necessarily ensure a write to disk.
    virtual void flush();

    /// Close file and release system resources.
    virtual void close() = 0;

    /// Report the location of the position cursor.
    /// @return Current file position.
    virtual off_t tell() const = 0;

    /// Move the position cursor to a different location.  The
    /// interpretation of \c offset is dependent on \c whence.  If \c
    /// whence is \c SEEK_SET, the \c offset is an absolute offset
    /// from the beginning of the file.  If \c whence is \c SEEK_CUR,
    /// \c offset is a relative offset from the current position.  If
    /// \c whence is \c SEEK_END, \c offset is an offset from the end
    /// of the file.
    virtual void seek(off_t offset, int whence) = 0;

    /// Get the name of the file, if available.  Not all implementors
    /// support this method.
    /// @return Name of the file, or an empty string if unavailable.
    virtual std::string getName() const;

    // Non-virtual convenience functions

    /// Read up to \c sz bytes, and store them into the buffer pointed
    /// to by \c dst.
    /// @return Number of bytes successfully read.
    inline size_t read(void * dst, size_t sz)
    {
        return read(dst, 1, sz);
    }

    /// Read up to \c sz - 1 characters and store them in \c dst.
    /// Reading stops after encountering a newline or EOF.  If a
    /// newline is read, it is included in \c dst.  A null character
    /// is stored after the last character in the \c dst buffer, and
    /// the length of the buffer is returned.  Since the buffer may
    /// contain nulls, it's better to trust the return value than
    /// strlen().
    /// @return Number of bytes stored in \c dst.  Zero indicates EOF.
    inline size_t readline(char * dst, size_t sz)
    {
        return readline(dst, sz, '\n');
    }

    /// Write \c sz bytes, taken from the buffer pointed to by \c src.
    /// @return Number of bytes successfully written.
    inline size_t write(void const * src, size_t sz)
    {
        return write(src, 1, sz);
    }

    /// Move the position cursor to \c offset bytes from the beginning
    /// of the file.
    inline void seek(off_t offset)
    {
        seek(offset, SEEK_SET);
    }

    
    //----------------------------------------------------------------------
    // Registration and dispatch

    /// Open a file handle for the given URI.  The flags field must be one of:
    ///   O_RDONLY : open the file read-only
    ///   O_WRONLY : open the file write-only
    ///   O_RDWR   : open the file for reading and writing
    /// The following optional modifier flags may be included with bitwise OR:
    ///   O_APPEND : seek to the end of the file on writes
    ///   O_TRUNC  : truncate file to length 0 if opened for writing
    ///   O_CREAT  : create file if it doesn't already exist
    ///   O_EXCL   : with O_CREAT, fail if the file already exists
    /// These flags are the usual UNIX fcntl.h values.  It is an error
    /// to use a fcntl flag not documented here.  Not all
    /// implementations of the File interface are required to support
    /// all possible flag combinations.  They will throw
    /// NotImplementedError if the given flag set is unsupported.
    static FilePtr open(std::string const & uri, int flags);

    /// Open an input file handle for the given URI.
    static FilePtr input(std::string const & uri)
    {
        return open(uri, O_RDONLY);
    }

    /// Open an output file handle for the given URI.
    static FilePtr output(std::string const & uri)
    {
        return open(uri, O_WRONLY | O_TRUNC | O_CREAT);
    }

    /// Open an appending output file handle for the given URI.
    static FilePtr append(std::string const & uri)
    {
        return open(uri, O_WRONLY | O_APPEND | O_CREAT);
    }

    /// Register a file opening handler for the given scheme.
    static void registerScheme(char const * scheme, FileOpenFunc * func);

    /// Set a default scheme for file URIs with no scheme otherwise
    /// specified.
    static void setDefaultScheme(char const * scheme);
};


#endif // WDS_FILE_H
