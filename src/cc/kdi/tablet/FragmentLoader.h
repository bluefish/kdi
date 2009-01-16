//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-12-05
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

#ifndef KDI_TABLET_FRAGMENTLOADER_H
#define KDI_TABLET_FRAGMENTLOADER_H

#include <boost/shared_ptr.hpp>
#include <string>

namespace kdi {
namespace tablet {

    class FragmentLoader;

    class Fragment;
    typedef boost::shared_ptr<Fragment> FragmentPtr;

} // namespace tablet
} // namespace kdi


//----------------------------------------------------------------------------
// FragmentLoader
//----------------------------------------------------------------------------
class kdi::tablet::FragmentLoader
{
public:
    /// Load a Fragment from a fragment URI.
    virtual FragmentPtr load(std::string const & fragmentUri) const = 0;

protected:
    ~FragmentLoader() {}
};

#endif // KDI_TABLET_FRAGMENTLOADER_H
