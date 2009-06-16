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
#include <kdi/server/Fragment.h>
#include <kdi/server/RestrictedFragment.h>
#include <warp/fs.h>
#include <warp/log.h>
#include <boost/enable_shared_from_this.hpp>
#include <cassert>

using namespace kdi::server;
typedef boost::mutex::scoped_lock lock_t;

//----------------------------------------------------------------------------
// FileTracker::Wrapper
//----------------------------------------------------------------------------
class FileTracker::Wrapper
    : public Fragment,
      public boost::enable_shared_from_this<FileTracker::Wrapper>
{
public:
    Wrapper(FileTracker * tracker, FragmentCPtr const & base) :
        tracker(tracker), base(base) {}

    ~Wrapper()
    {
        std::string filename = base->getFilename();
        base.reset();
        tracker->release(filename);
    }

public:                         // Fragment API
    virtual void exportFragment() const
    {
        tracker->setExported(getFilename());
        base->exportFragment();
    }
    
    virtual std::string getFilename() const
    {
        return base->getFilename();
    }

    virtual size_t getDataSize() const
    {
        return base->getDataSize();
    }

    virtual size_t getPartialDataSize(
        warp::Interval<std::string> const & rows) const
    {
        return base->getPartialDataSize(rows);
    }

    virtual void getColumnFamilies(
        std::vector<std::string> & families) const
    {
        return base->getColumnFamilies(families);
    }

    virtual FragmentCPtr getRestricted(
        std::vector<std::string> const & families) const
    {
        return makeRestrictedFragment(
            shared_from_this(),
            families);
    }

    virtual size_t nextBlock(ScanPredicate const & pred,
                             size_t minBlock) const
    {
        return base->nextBlock(pred, minBlock);
    }

    virtual std::auto_ptr<FragmentBlock>
    loadBlock(size_t blockAddr) const
    {
        return base->loadBlock(blockAddr);
    }

private:
    FileTracker * const tracker;
    FragmentCPtr base;
};


//----------------------------------------------------------------------------
// FileTracker
//----------------------------------------------------------------------------
FileTracker::FileTracker(FragmentLoader * loader,
                         std::string const & rootDir) :
    loader(loader), rootDir(rootDir), nUnknown(0)
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

FragmentCPtr FileTracker::load(std::string const & filename)
{
    FragmentCPtr p(new Wrapper(this, loader->load(filename)));
    {
        boost::mutex::scoped_lock lock(mutex);
        fileMap[p->getFilename()].incrementCount();
    }
    return p;
}

void FileTracker::setExported(std::string const & filename)
{
    lock_t lock(mutex);
    
    filemap_t::iterator i = fileMap.find(filename);
    assert(i != fileMap.end());

    i->second.setExported();
}

void FileTracker::release(std::string const & filename)
{
    lock_t lock(mutex);
    
    while(nUnknown)
        cond.wait(lock);

    filemap_t::iterator i = fileMap.find(filename);
    assert(i != fileMap.end());

    i->second.decrementCount();
    if(i->second.getCount())
        return;

    bool isExported = i->second.isExported();

    fileMap.erase(i);
    lock.unlock();

    if(!isExported)
    {
        warp::log("Removing local file: %s", filename);
        warp::fs::remove(
            warp::fs::resolve(rootDir, filename));
    }
}

void FileTracker::pendingAssigned(PendingFile * p)
{
    assert(p->tracker == this);
    warp::Timestamp ctime = warp::fs::creationTime(
        warp::fs::resolve(rootDir, p->name));

    lock_t lock(mutex);

    assert(pendingCTimes.find(p->name) == pendingCTimes.end());
    assert(nUnknown);

    pendingCTimes[p->name] = ctime;
    --nUnknown;

    fileMap[p->name].incrementCount();

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

        lock.unlock();
        release(p->name);
    }
    else
    {
        assert(nUnknown);
        --nUnknown;
        cond.notify_all();
    }
}
