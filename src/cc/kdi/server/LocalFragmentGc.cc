//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-06-15
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

#include <kdi/server/LocalFragmentGc.h>
#include <kdi/server/Fragment.h>
#include <kdi/server/FragmentRemover.h>
#include <kdi/server/RestrictedFragment.h>
#include <warp/log.h>
#include <boost/enable_shared_from_this.hpp>

using namespace kdi::server;
using warp::log;

//----------------------------------------------------------------------------
// LocalFragmentGc::Wrapper
//----------------------------------------------------------------------------
class LocalFragmentGc::Wrapper
    : public Fragment,
      public boost::enable_shared_from_this<LocalFragmentGc::Wrapper>
{
public:
    Wrapper(LocalFragmentGc * gc, FragmentCPtr const & base) :
        gc(gc), base(base) {}

    ~Wrapper()
    {
        std::string filename = base->getFilename();
        base.reset();
        gc->release(filename);
    }

public:                         // Fragment API
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
    LocalFragmentGc * const gc;
    FragmentCPtr base;
};

//----------------------------------------------------------------------------
// LocalFragmentGc
//----------------------------------------------------------------------------
LocalFragmentGc::LocalFragmentGc(FragmentRemover * remover) :
    remover(remover)
{
}

FragmentCPtr LocalFragmentGc::wrapLocalFragment(FragmentCPtr const & fragment)
{
    if(!remover)
        return fragment;

    log("LocalGc: wrapping %s", fragment->getFilename());

    FragmentCPtr p(new Wrapper(this, fragment));
    {
        boost::mutex::scoped_lock lock(mutex);
        localSet.insert(p->getFilename());
    }
    return p;
}
    
void LocalFragmentGc::exportFragment(FragmentCPtr const & fragment)
{
    log("LocalGc: exporting %s", fragment->getFilename());

    boost::mutex::scoped_lock lock(mutex);
    localSet.erase(fragment->getFilename());
}

void LocalFragmentGc::exportAll()
{
    log("LocalGc: export all");

    boost::mutex::scoped_lock lock(mutex);
    localSet.clear();
}

void LocalFragmentGc::release(std::string const & filename)
{
    log("LocalGc: release %s", filename);

    size_t nErased;
    {
        boost::mutex::scoped_lock lock(mutex);
        nErased = localSet.erase(filename);
    }
    if(nErased)
    {
        log("LocalGc: deleting local fragment %s", filename);
        remover->remove(filename);
    }
}
