//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-20
// 
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the Free Software
// Foundation; either version 2 of the License, or any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
// details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
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
