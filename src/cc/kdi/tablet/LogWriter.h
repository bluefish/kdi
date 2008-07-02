//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/LogWriter.h $
//
// Created 2008/05/20
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_TABLET_LOGWRITER_H
#define KDI_TABLET_LOGWRITER_H

#include <oort/buildstream.h>
#include <kdi/cell.h>
#include <warp/file.h>
#include <string>
#include <vector>

namespace kdi {
namespace tablet {

    class LogWriter;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// LogWriter
//----------------------------------------------------------------------------
class kdi::tablet::LogWriter
{
    oort::BuildStreamHandle logStream;
    warp::FilePtr fp;
    std::string uri;

public:
    explicit LogWriter(std::string const & fileUri);

    /// Sync log to disk.
    void sync();

    /// Write a sequence of cells under a named log entry.  The name
    /// represents the Tablet to which the cells belong.
    void logCells(std::string const & tabletName,
                  std::vector<Cell> const & cells);

    /// Get the sharedlog: URI for this log file.
    std::string const & getUri() const { return uri; }
};


#endif // KDI_TABLET_LOGWRITER_H
