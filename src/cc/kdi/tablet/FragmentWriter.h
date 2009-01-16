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

#ifndef KDI_TABLET_FRAGMENTWRITER_H
#define KDI_TABLET_FRAGMENTWRITER_H

#include <kdi/cell.h>
#include <string>

namespace kdi {
namespace tablet {

    class FragmentWriter;

} // namespace tablet
} // namespace kdi


//----------------------------------------------------------------------------
// FragmentWriter
//----------------------------------------------------------------------------
class kdi::tablet::FragmentWriter
{
public:
    /// Start a new fragment file for the given table (or locality
    /// group).
    virtual void start(std::string const & table) = 0;

    /// Put more data in the output.  Cells must be added in strictly
    /// increasing cell order.
    virtual void put(Cell const & x) = 0;

    /// Finalize the output file and return the URI to newly created
    /// fragment.  Nothing more can be added before a new call to
    /// open().
    virtual std::string finish() = 0;

    /// Get an estimate of the output file size if finish() were to be
    /// called without adding any more data with put().
    virtual size_t size() const = 0;

protected:
    ~FragmentWriter() {}
};

#endif // KDI_TABLET_FRAGMENTWRITER_H
