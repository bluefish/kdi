//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-08
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

#ifndef KDI_TABLET_TABLETGC_H
#define KDI_TABLET_TABLETGC_H

#include <kdi/table.h>
#include <warp/timestamp.h>
#include <string>
#include <vector>

namespace kdi {
namespace tablet {

    class TabletGc;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// TabletGc
//----------------------------------------------------------------------------
class kdi::tablet::TabletGc
{
    std::string dataDir;
    kdi::TablePtr metaTable;
    warp::Timestamp beforeTime;
    std::string serverRestriction;

public:
    /// Scan a tablet server data directory looking for garbage data
    /// files.  "Garbage" is any file in the data directory no longer
    /// needed for the correct operation of any tablet server
    /// operating from that directory.
    ///
    /// To find the garbage set, we find the set of all files in the
    /// directory and subtract the set of active files.  The active
    /// set is built by scanning live tablet config data from the META
    /// table and in per-directory state files.
    ///
    /// Because the server can be creating new files all the time, it
    /// is important to restrict the file set under consideration to
    /// those older than a certain time.  The time should be chosen
    /// such that no temporary new file can be older than it.  A good
    /// choice would be the startup time of the current server
    /// instance.
    static void findTabletGarbage(
        std::string const & dataDir,
        kdi::TablePtr const & metaTable,
        warp::Timestamp const & beforeTime,
        std::string const & serverRestriction,
        std::vector<std::string> & garbageFiles);

public:
    TabletGc(std::string const & dataDir,
             kdi::TablePtr const & metaTable,
             warp::Timestamp const & beforeTime,
             std::string const & serverRestriction);

    /// Run Tablet GC
    void run() const;
};

#endif // KDI_TABLET_TABLETGC_H
