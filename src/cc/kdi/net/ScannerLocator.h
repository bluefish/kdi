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

#ifndef KDI_NET_SCANNERLOCATOR_H
#define KDI_NET_SCANNERLOCATOR_H

#include <warp/lru_cache.h>
#include <Ice/ServantLocator.h>
#include <boost/thread/mutex.hpp>

namespace kdi {
namespace net {

    class ScannerLocator;

} // namespace net
} // namespace kdi

//----------------------------------------------------------------------------
// ScannerLocator
//----------------------------------------------------------------------------
class kdi::net::ScannerLocator
    : public Ice::ServantLocator
{
    warp::LruCache<size_t,Ice::ObjectPtr> cache;

    Ice::ObjectPtr * mark;
    boost::mutex mutex;

    typedef boost::mutex::scoped_lock lock_t;

public:
    explicit ScannerLocator(size_t nObjects);

    /// Add a scanner to the cache -- returns the number of active scanners
    size_t add(size_t id, Ice::ObjectPtr const & ptr);

    /// Remove a scanner from the cache
    void remove(size_t id);

    /// Purge everything that has not been used since the last mark
    /// and set a new mark.
    void purgeAndMark();

    // ServantLocator API
    virtual Ice::ObjectPtr locate(Ice::Current const & cur,
                                  Ice::LocalObjectPtr & cookie);
    virtual void finished(Ice::Current const & cur,
                          Ice::ObjectPtr const & servant,
                          Ice::LocalObjectPtr const & cookie);
    virtual void deactivate(std::string const & category);
};

#endif // KDI_NET_SCANNERLOCATOR_H
