//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-08-16
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

#ifndef WARP_FILESTREAM_H
#define WARP_FILESTREAM_H

#include <iosfwd>
#include <boost/iostreams/categories.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/stream_buffer.hpp>
#include <warp/file.h>
#include <ex/exception.h>

namespace warp
{
    /// Adapter modeling SeekableDevice concept from Boost Iostreams.
    class FileDevice
    {
        FilePtr fp;

    public:
        typedef char char_type;
        struct category :
            boost::iostreams::seekable_device_tag,
            boost::iostreams::closable_tag,
            boost::iostreams::flushable_tag {};
        
    public:
        explicit FileDevice(FilePtr const & fp) :
            fp(fp)
        {
            using namespace ex;
            if(!fp)
                raise<ValueError>("null File");
        }

        std::streamsize read(char * s, std::streamsize n)
        {
            size_t sz = fp->read(s, n);
            if(sz == 0)
                return -1;
            return sz;
        }

        std::streamsize write(char const * s, std::streamsize n)
        {
            size_t sz = fp->write(s, n);
            if(sz == 0)
                return -1;
            return sz;
        }

        std::streampos seek(std::streamoff off, std::ios_base::seekdir way)
        {
            using namespace ex;

            if(way == std::ios_base::beg)
                fp->seek(off, SEEK_SET);
            else if(way == std::ios_base::cur)
                fp->seek(off, SEEK_CUR);
            else if(way == std::ios_base::end)
                fp->seek(off, SEEK_END);
            else
                raise<ValueError>("unknown seek mode: %s", way);
        
            return fp->tell();
        }

        void close()
        {
            fp->close();
        }

        bool flush()
        {
            fp->flush();
            return true;
        }
    };

    /// Adapter derived from std::iostream that wraps a FilePtr.
    typedef boost::iostreams::stream<FileDevice> FileStream;

    /// Adapter derived from std::streambuf that wraps a FilePtr.
    typedef boost::iostreams::stream_buffer<FileDevice> FileStreamBuffer;
}


#endif // WARP_FILESTREAM_H
