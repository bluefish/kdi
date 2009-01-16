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

#include <kdi/tablet/SwitchedFragmentLoader.h>
#include <warp/uri.h>
#include <ex/exception.h>

using namespace kdi::tablet;
using namespace warp;
using namespace ex;

void SwitchedFragmentLoader::setLoader(
    std::string const & scheme, FragmentLoader * loader)
{
    EX_CHECK_NULL(loader);
    loaders[scheme] = loader;
}

FragmentPtr SwitchedFragmentLoader::load(std::string const & uri) const
{
    map_t::const_iterator i = loaders.find(uriTopScheme(uri));
    if(i == loaders.end())
        raise<ValueError>("unknown fragment scheme: %s", uri);

    return i->second->load(uri);
}
