//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-08-07
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

#include <kdi/tablet/ConfigManager.h>
#include <kdi/tablet/CachedLogLoader.h>
#include <kdi/tablet/DiskFragment.h>
#include <warp/uri.h>
#include <warp/log.h>
#include <ex/exception.h>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace ex;

//----------------------------------------------------------------------------
// ConfigManager
//----------------------------------------------------------------------------
ConfigManager::ConfigManager() :
    logLoader(new CachedLogLoader)
{
}

FragmentPtr ConfigManager::openFragment(std::string const & uri)
{
    log("ConfigManager: openFragment %s", uri);

    std::string scheme = uriTopScheme(uri);
    if(scheme == "disk")
    {
        // Disk table: open and return
        FragmentPtr p(new DiskFragment(uri));
        return p;
    }
    else if(scheme == "sharedlog")
    {
        // Log table: serialize to a disk table and return that.
        std::string diskUri = logLoader->getDiskUri(uri, this);
        FragmentPtr p(new DiskFragment(diskUri));
        return p;
    }

    // Unknown scheme
    raise<RuntimeError>("unknown fragment scheme %s: %s", scheme, uri);
}
