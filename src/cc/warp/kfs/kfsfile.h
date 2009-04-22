//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/main/cosmix/src/cc/warp/kfs/kfsfile.h#6 $
//
// Created 2006/06/27
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// \brief A file wrapper over the KFS client library
//
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
