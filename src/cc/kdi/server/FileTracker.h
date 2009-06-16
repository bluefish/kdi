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

#include <kdi/server/FragmentLoader.h>
#include <warp/timestamp.h>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <string>
#include <tr1/unordered_map>

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
    : public kdi::server::FragmentLoader,
      private boost::noncopyable
{
public:
    FileTracker(FragmentLoader * loader, std::string const & rootDir);

    /// Create a new pending file and return it.
    PendingFilePtr createPending();

    /// Get oldest creation timestamp of all pending files.  If there
    /// are no pending files, return Timestamp::MAX_TIME.  This
    /// function will block while there are unassigned pending files
    /// in existence.
    warp::Timestamp getOldestPending() const;

public:                     // FragmentLoader API

    /// Load the fragment from the given file.  The loaded fragment
    /// will be tracked so that its backing file will automatically be
    /// deleted when the wrapped handle is destroyed.  To avoid this,
    /// call exportFragment() on the loaded Fragment.  Exported
    /// fragment files will be left to a global garbage collection
    /// process.
    virtual FragmentCPtr load(std::string const & filename);

private:
    class Wrapper;

    class RefCount
    {
        uint32_t x;
    public:
        RefCount() : x(0) {}

        bool isExported() const { return (x & 0x80000000) != 0; }
        void setExported() { x |= 0x80000000; }
        
        size_t getCount() const { return x & 0x7fffffff; }
        void incrementCount() { ++x; }
        void decrementCount() { --x; }
    };

    typedef std::tr1::unordered_map<std::string, RefCount> filemap_t;
    typedef std::tr1::unordered_map<std::string, warp::Timestamp> timemap_t;

private:
    /// Mark a filename as exported.  Exported files will be left for
    /// the global file GC instead of being automatically deleted.
    void setExported(std::string const & filename);

    /// Release the given filename.  If the file has not been
    /// previously exported, it will be deleted.
    void release(std::string const & filename);

private:
    friend class PendingFile;

    /// Remove pending from unknown count and add it to the ctime map.
    void pendingAssigned(PendingFile * p);

    /// If pending has been assigned, remove it from the ctime map.
    /// Otherwise, remove it from the unknown count.
    void pendingReleased(PendingFile * p);

private:
    FragmentLoader * const loader;
    std::string const rootDir;
    
    // Number of pending files that have yet to be assigned a name.
    size_t nUnknown;

    // Contains (filename -> Timestamp)
    // The creation time of outstanding named PendingFiles.
    timemap_t pendingCTimes;

    // Contains (filename -> RefCount)
    // Every time we hear about a file (through a PendingFile or a
    // loaded Fragment) we add a reference.  When a PendingFile or
    // Fragment goes away, we remove a reference.  If the refcount
    // goes to zero, we remove the file from the map.  If the file has
    // not been exported, we also delete the file.
    filemap_t fileMap;

    mutable boost::mutex mutex;
    mutable boost::condition cond;
};

#endif // KDI_SERVER_FILETRACKER_H
