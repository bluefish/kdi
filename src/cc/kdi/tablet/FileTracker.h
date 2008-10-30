//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-08-12
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

#ifndef KDI_TABLET_FILETRACKER_H
#define KDI_TABLET_FILETRACKER_H

#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>
#include <string>
#include <map>

namespace kdi {
namespace tablet {

    class FileTracker;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// FileTracker
//----------------------------------------------------------------------------
class kdi::tablet::FileTracker
    : private boost::noncopyable
{
    typedef std::map<std::string, size_t> map_t;
    typedef boost::mutex::scoped_lock lock_t;

    map_t files;
    boost::mutex mutex;

public:
    /// Scoped file tracking
    class AutoTracker
    {
        FileTracker & tracker;
        std::string filename;

    public:
        AutoTracker(FileTracker & tracker, std::string const & filename) :
            tracker(tracker), filename(filename)
        {
            tracker.track(filename);
        }
        ~AutoTracker()
        {
            tracker.release(filename);
        }
    };

public:
    /// Track a file for automatic deletion.  Sets the reference count
    /// of the file to 1.  When the reference count drops to 0
    /// (through calls to release()), the file will be deleted unless
    /// it is untrack()'ed before then.
    void track(std::string const & filename);

    /// Remove a file from automatic deletion tracking.
    void untrack(std::string const & filename);

    /// Add a reference to a file tracked for automatic deletion.
    void addReference(std::string const & filename);

    /// Release a reference on a tracked file.  If this causes the
    /// reference count to drop to 0, the file will be deleted.
    void release(std::string const & filename);
};


#endif // KDI_TABLET_FILETRACKER_H
