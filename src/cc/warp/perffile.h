//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/perffile.h#1 $
//
// Created 2006/12/08
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_PERFFILE_H
#define WARP_PERFFILE_H

#include "file.h"
#include "perfvar.h"
#include <boost/utility.hpp>

namespace warp
{
    class PerformanceFile;
}

//----------------------------------------------------------------------------
// PerformanceFile
//----------------------------------------------------------------------------
/// Performance monitored File interface.
class warp::PerformanceFile : public warp::File, private boost::noncopyable
{
    FilePtr fp;
    PerformanceInteger bytesIn;
    PerformanceInteger bytesOut;

    void setupLogging(PerformanceLog * log);

public:
    /// Construct a PerformanceFile wrapper over the given file
    /// pointer.  Register with the default PerformanceLog.
    explicit PerformanceFile(FilePtr const & fp);

    /// Construct a PerformanceFile wrapper over the given file
    /// pointer.  Register with the given PerformanceLog.
    explicit PerformanceFile(FilePtr const & fp, PerformanceLog * log);

    virtual size_t read(void * dst, size_t elemSz, size_t nElem);
    virtual size_t readline(char * dst, size_t sz, char delim);
    virtual size_t write(void const * src, size_t elemSz, size_t nElem);
    virtual void flush();
    virtual void close();
    virtual off_t tell() const;
    virtual void seek(off_t offset, int whence);
    virtual std::string getName() const;
};

#endif // WARP_PERFFILE_H
