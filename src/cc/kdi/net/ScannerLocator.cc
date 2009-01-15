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

using namespace kdi;
using namespace kdi::net;

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
    cache(nObjects) {}

size_t ScannerLocator::add(size_t id, Ice::ObjectPtr const & ptr)
{
    lock_t lock(mutex);
    Ice::ObjectPtr * p = cache.get(id);
    *p = ptr;
    cache.release(p);
    return cache.size();
}

void ScannerLocator::remove(size_t id)
{
    lock_t lock(mutex);
    cache.remove(id);
}

Ice::ObjectPtr ScannerLocator::locate(Ice::Current const & cur,
                                      Ice::LocalObjectPtr & cookie)
{
    size_t id;
    if(!warp::parseInt(id, cur.id.name))
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
    cache.removeAll();
}
