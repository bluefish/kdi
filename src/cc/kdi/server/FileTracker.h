//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-20
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

#ifndef KDI_SERVER_FILETRACKER_H
#define KDI_SERVER_FILETRACKER_H

#include <warp/timestamp.h>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <string>
#include <map>

namespace kdi {
namespace server {

    class FileTracker;

    // Forward declarations
    class PendingFile;
    typedef boost::shared_ptr<PendingFile> PendingFilePtr;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// FileTracker
//----------------------------------------------------------------------------
class kdi::server::FileTracker
    : private boost::noncopyable
{
public:
    explicit FileTracker(std::string const & root);

    /// Create a new pending file and return it.
    PendingFilePtr createPending();

    /// Get oldest creation timestamp of all pending files.  If there
    /// are no pending files, return Timestamp::MAX_TIME.  This
    /// function will block while there are unassigned pending files
    /// in existence.
    warp::Timestamp getOldestPending() const;

private:
    friend class PendingFile;

    /// Remove pending from unknown count and add it to the ctime map.
    void pendingAssigned(PendingFile * p);

    /// If pending has been assigned, remove it from the ctime map.
    /// Otherwise, remove it from the unknown count.
    void pendingReleased(PendingFile * p);

private:
    typedef std::map<std::string, warp::Timestamp> timemap_t;

    std::string const root;
    size_t nUnknown;
    timemap_t pendingCTimes;
    mutable boost::mutex mutex;
    mutable boost::condition cond;
};

#endif // KDI_SERVER_FILETRACKER_H
