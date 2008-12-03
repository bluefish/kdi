//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-12-02
// 
// This file is part of the warp library.
// 
// The warp library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The warp library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef WARP_CALL_OR_DIE_H
#define WARP_CALL_OR_DIE_H

#include <boost/function.hpp>
#include <string>

namespace warp {

    /// Wrap a function call so that any uncaught exceptions will be
    /// logged and cause the process to terminate.  This is useful for
    /// wrapping background thread loops.  The given name is used as a
    /// label for any log messages.  If verbose is true, the wrapper
    /// function will also log normal start and exit from the
    /// function.
    boost::function<void ()> callOrDie(
        boost::function<void ()> const & func,
        std::string const & name, bool verbose);

} // namespace warp

#endif // WARP_CALL_OR_DIE_H
