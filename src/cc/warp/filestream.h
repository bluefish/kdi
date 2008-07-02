//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/filestream.h#1 $
//
// Created 2007/08/16
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_FILESTREAM_H
#define WARP_FILESTREAM_H

#include <iosfwd>
#include <boost/iostreams/categories.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/stream_buffer.hpp>
#include "warp/file.h"
#include "ex/exception.h"

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
