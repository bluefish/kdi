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

#include <kdi/tablet/FileTracker.h>
#include <warp/fs.h>
#include <warp/log.h>
#include <ex/exception.h>

using namespace kdi::tablet;
using namespace warp;
using namespace ex;

//----------------------------------------------------------------------------
// FileTracker
//----------------------------------------------------------------------------
FileTracker::FileTracker()
{
    log("FileTracker %p: created", this);
}

FileTracker::~FileTracker()
{
    log("FileTracker %p: destroyed", this);
}

void FileTracker::track(std::string const & filename)
{
    log("FileTracker::track: %s", filename);

    lock_t lock(mutex);

    // Get the refCount.  This will be zero for a file we haven't
    // seen before.
    size_t & refCount = files[filename];
    if(refCount)
        raise<RuntimeError>("already tracking: %s", filename);

    refCount = 1;
}

void FileTracker::untrack(std::string const & filename)
{
    log("FileTracker::untrack: %s", filename);

    lock_t lock(mutex);

    // Remove file from tracked list.  No problem if it isn't there.
    files.erase(filename);
}

void FileTracker::addReference(std::string const & filename)
{
    lock_t lock(mutex);

    // Add a reference to a file if it is already in the tracked list.
    map_t::iterator i = files.find(filename);
    if(i != files.end())
    {
        ++i->second;
        log("FileTracker::addReference: %s (ref=%d)", filename, i->second);
    }
    else
        log("FileTracker::addReference: %s (not tracked)", filename);
}

void FileTracker::release(std::string const & filename)
{
    lock_t lock(mutex);

    // Find the file
    map_t::iterator i = files.find(filename);

    // If it's not in the map, it's not tracked for deletion
    if(i == files.end())
    {
        log("FileTracker::release: %s (not tracked)", filename);
        return;
    }
        
    // Decrement refCount.  If it still has references, stop.
    if(--i->second)
    {
        log("FileTracker::release: %s (ref=%d)", filename, i->second);
        return;
    }

    // Delete the file and remove it from the tracking list.
    log("FileTracker::release: %s (ref=0) DELETE", filename);
    fs::remove(filename);
    files.erase(i);
}

