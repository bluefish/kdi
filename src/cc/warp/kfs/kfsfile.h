//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-06-27
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

#ifndef WARP_KFSFILE_H
#define WARP_KFSFILE_H

#include <kfs/KfsClient.h>
#include <warp/file.h>
#include <string>
#include <boost/noncopyable.hpp>

namespace warp {
namespace kfs {

    class KfsFile;

} // namespace kfs
} // namespace warp


//----------------------------------------------------------------------------
// KfsFile
//----------------------------------------------------------------------------
class warp::kfs::KfsFile
    : public warp::File,
      private boost::noncopyable
{
public:
    KfsFile(KFS::KfsClientPtr const & client,
            std::string const & uri, int mode);
    ~KfsFile();

    // File interface
    size_t read(void * dst, size_t elemSz, size_t nElem);
    size_t readline(char * dst, size_t sz, char delim);
    size_t write(void const * src, size_t elemSz, size_t nElem);
    void flush();
    void close();
    off_t tell() const;
    void seek(off_t offset, int whence);
    std::string getName() const { return fn; }

private:
    KFS::KfsClientPtr client;
    int fd;
    std::string fn;
};

#endif // WARP_KFSFILE_H
