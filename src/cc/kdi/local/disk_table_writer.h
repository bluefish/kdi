//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/local/disk_table_writer.h#1 $
//
// Created 2007/10/04
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_LOCAL_DISK_TABLE_WRITER_H
#define KDI_LOCAL_DISK_TABLE_WRITER_H

#include <kdi/cell.h>
#include <boost/scoped_ptr.hpp>
#include <boost/noncopyable.hpp>

namespace kdi {
namespace local {
    
    /// Serialize an ordered sequence of cells to a file with an index.
    class DiskTableWriter;

} // namespace local
} // namespace kdi

//----------------------------------------------------------------------------
// DiskTableWriter
//----------------------------------------------------------------------------
class kdi::local::DiskTableWriter
    : public CellStream
{
    class Impl;
    boost::scoped_ptr<Impl> impl;

public:
    DiskTableWriter(std::string const & fn, size_t blockSize);
    ~DiskTableWriter();

    void close();
    void put(Cell const & x);
};

#endif // KDI_LOCAL_DISK_TABLE_WRITER_H
