//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-01
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

#ifndef KDI_SERVER_CACHEDFRAGMENTLOADER_H
#define KDI_SERVER_CACHEDFRAGMENTLOADER_H

#include <kdi/server/FragmentLoader.h>
#include <tr1/unordered_map>
#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/weak_ptr.hpp>

namespace kdi {
namespace server {

    /// A cache for loading fragments.  If a fragment has already been
    /// loaded, it should be reused.
    class CachedFragmentLoader;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// CachedFragmentLoader
//----------------------------------------------------------------------------
class kdi::server::CachedFragmentLoader
    : public kdi::server::FragmentLoader,
      private boost::noncopyable
{
public:
    CachedFragmentLoader(FragmentLoader * baseLoader);
    virtual FragmentCPtr load(std::string const & filename);

private:
    typedef boost::weak_ptr<Fragment const> FragmentWeakCPtr;
    typedef std::tr1::unordered_map<std::string, FragmentWeakCPtr> map_t;
    
    FragmentLoader * baseLoader;
    map_t cache;
    size_t nextPurgeSize;
    boost::mutex mutex;

private:
    void purgeStaleEntries();
};

#endif // KDI_SERVER_CACHEDFRAGMENTLOADER_H
