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

#include <kdi/server/FileTracker.h>
#include <kdi/server/PendingFile.h>
#include <warp/fs.h>
#include <cassert>

using namespace kdi::server;
typedef boost::mutex::scoped_lock lock_t;

//----------------------------------------------------------------------------
// FileTracker
//----------------------------------------------------------------------------
FileTracker::FileTracker(std::string const & root) :
    root(root), nUnknown(0)
{
}

PendingFilePtr FileTracker::createPending()
{
    {
        lock_t lock(mutex);
        ++nUnknown;
    }

    PendingFilePtr p(new PendingFile(this));
    return p;
}

warp::Timestamp FileTracker::getOldestPending() const
{
    lock_t lock(mutex);

    while(nUnknown)
        cond.wait(lock);

    warp::Timestamp oldest = warp::Timestamp::MAX_TIME;
    for(timemap_t::const_iterator i = pendingCTimes.begin();
        i != pendingCTimes.end(); ++i)
    {
        if(i->second < oldest)
            oldest = i->second;
    }
    return oldest;
}

void FileTracker::pendingAssigned(PendingFile * p)
{
    assert(p->tracker == this);
    warp::Timestamp ctime = warp::fs::creationTime(
        warp::fs::resolve(root, p->name));

    lock_t lock(mutex);

    assert(pendingCTimes.find(p->name) == pendingCTimes.end());
    assert(nUnknown);

    pendingCTimes[p->name] = ctime;
    --nUnknown;
    cond.notify_all();
}

void FileTracker::pendingReleased(PendingFile * p)
{
    assert(p->tracker == this);

    lock_t lock(mutex);

    if(p->nameSet)
    {
        assert(pendingCTimes.find(p->name) != pendingCTimes.end());
        pendingCTimes.erase(p->name);
    }
    else
    {
        assert(nUnknown);
        --nUnknown;
        cond.notify_all();
    }
}
