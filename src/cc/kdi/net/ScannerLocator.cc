//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-14
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

#include <kdi/net/ScannerLocator.h>
#include <warp/strutil.h>
#include <warp/file.h>
#include <ex/exception.h>
#include <sstream>

using namespace kdi;
using namespace kdi::net;
using namespace ex;

namespace {

    struct CacheCookie : public Ice::LocalObject
    {
        Ice::ObjectPtr * ptr;
        CacheCookie(Ice::ObjectPtr * ptr) : ptr(ptr) {}
    };

}

//----------------------------------------------------------------------------
// ScannerLocator
//----------------------------------------------------------------------------
ScannerLocator::ScannerLocator(size_t nObjects) :
    cache(nObjects+1),
    mark(0)
{
    size_t salt;
    if(!warp::File::input("/dev/random")->read(&salt, sizeof(salt), 1))
        raise<RuntimeError>("couldn't generate scanner instance ID");

    std::ostringstream oss;
    oss << std::hex << salt;
    instanceId = oss.str();
}

std::string ScannerLocator::getNameFromId(size_t id) const
{
    std::ostringstream oss;
    oss << instanceId << ':' << id;
    return oss.str();
}

size_t ScannerLocator::getIdFromName(std::string const & name) const
{
    char const * begin = name.c_str();
    char const * end = begin + name.size();
    char const * sep = std::find(begin, end, ':');

    size_t id;
    if(sep != end &&
       instanceId == warp::StringRange(begin,sep) &&
       warp::parseInt(id, sep+1, end))
    {
        return id;
    }

    return size_t(-1);
}

size_t ScannerLocator::add(size_t id, Ice::ObjectPtr const & ptr)
{
    if(id == size_t(-1))
        raise<RuntimeError>("can't have scanner ID -1");

    lock_t lock(mutex);
    Ice::ObjectPtr * p = cache.get(id);
    *p = ptr;
    cache.release(p);
    return cache.size() - (mark ? 1 : 0);
}

void ScannerLocator::remove(size_t id)
{
    if(id == size_t(-1))
        raise<RuntimeError>("can't have scanner ID -1");

    lock_t lock(mutex);
    cache.remove(id);
}

void ScannerLocator::purgeAndMark()
{
    lock_t lock(mutex);
    if(mark)
    {
        cache.removeUntil(mark);
        cache.release(mark);
        mark = 0;
    }
    mark = cache.get(size_t(-1));
}

Ice::ObjectPtr ScannerLocator::locate(Ice::Current const & cur,
                                      Ice::LocalObjectPtr & cookie)
{
    size_t id = getIdFromName(cur.id.name);
    if(id == size_t(-1))
        return 0;

    lock_t lock(mutex);
    if(cache.contains(id))
    {
        Ice::ObjectPtr * p = cache.get(id);
        cookie = new CacheCookie(p);
        return *p;
    }
    else
    {
        return 0;
    }
}

void ScannerLocator::finished(Ice::Current const & cur,
                              Ice::ObjectPtr const & servant,
                              Ice::LocalObjectPtr const & cookie)
{
    lock_t lock(mutex);
    cache.release(static_cast<CacheCookie const *>(&*cookie)->ptr);
}

void ScannerLocator::deactivate(std::string const & category)
{
    lock_t lock(mutex);
    if(mark)
    {
        cache.release(mark);
        mark = 0;
    }
    cache.removeAll();
}
