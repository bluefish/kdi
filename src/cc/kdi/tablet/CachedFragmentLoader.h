//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-11-20
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

#ifndef KDI_TABLET_CACHEDFRAGMENTLOADER_H
#define KDI_TABLET_CACHEDFRAGMENTLOADER_H

#include <kdi/tablet/FragmentLoader.h>
#include <kdi/tablet/forward.h>
#include <tr1/unordered_map>
#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>

namespace kdi {
namespace tablet {

    /// A cache for loading fragments.  If a fragment has already been
    /// loaded, it should be reused.
    class CachedFragmentLoader
        : public FragmentLoader,
          private boost::noncopyable
    {
        typedef std::tr1::unordered_map<std::string, FragmentWeakPtr> map_t;

        FragmentLoader * loader;
        mutable map_t cache;
        mutable size_t nextPurgeSize;
        mutable boost::mutex mutex;

    public:
        CachedFragmentLoader(FragmentLoader * loader);
        FragmentPtr load(std::string const & uri) const;

    private:
        void purgeStaleEntries() const;
    };

} // namespace tablet
} // namespace kdi


#endif // KDI_TABLET_CACHEDFRAGMENTLOADER_H
