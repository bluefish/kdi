//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-15
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

#include <kdi/tablet/DiskFragmentLoader.h>
#include <kdi/tablet/DiskFragment.h>
#include <warp/log.h>
#include <warp/uri.h>
#include <ex/exception.h>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace ex;

namespace
{
    template <class T>
    class EmptyScan : public flux::Stream<T>
    {
    public:
        bool get(T & x) { return false; }
    };

    /// Placeholder fragment with empty data.  This is used in place
    /// of a DiskFragment if we're unable to load the disk fragment.
    class EmptyFragment : public Fragment
    {
        std::string uri;

    public:
        explicit EmptyFragment(std::string const & uri) : uri(uri) {}

        CellStreamPtr scan(ScanPredicate const & pred) const
        {
            CellStreamPtr p(new EmptyScan<Cell>);
            return p;
        }

        bool isImmutable() const
        {
            return true;
        }

        std::string getFragmentUri() const
        {
            return uri;
        }

        std::string getDiskUri() const
        {
            return uriPopScheme(uri);
        }

        size_t getDiskSize(warp::Interval<std::string> const & rows) const
        {
            return 0;
        }
        
        flux::Stream< std::pair<std::string, size_t> >::handle_t
        scanIndex(warp::Interval<std::string> const & rows) const
        {
            flux::Stream< std::pair<std::string, size_t> >::handle_t p(
                new EmptyScan< std::pair<std::string, size_t> >
                );
            return p;
        }
    };
}


//----------------------------------------------------------------------------
// DiskFragmentLoader
//----------------------------------------------------------------------------
FragmentPtr DiskFragmentLoader::load(std::string const & uri) const
{
    if(uriTopScheme(uri) != "disk")
        raise<ValueError>("invalid URI for DiskFragmentLoader: %s", uri);

    log("DiskFragmentLoader: loading %s", uri);
    FragmentPtr ptr;
    try {
        ptr.reset(new DiskFragment(uri));
    }
    catch(std::exception const & ex) {
        // Load failed -- make an empty placeholder
        log("ERROR: failed to load %s: %s", uri, ex.what());
        ptr.reset(new EmptyFragment(uri));
    }

    return ptr;
}
