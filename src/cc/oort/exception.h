//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-11
// 
// This file is part of the oort library.
// 
// The oort library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The oort library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef OORT_EXCEPTION_H
#define OORT_EXCEPTION_H

#include <ex/exception.h>

namespace oort
{
    EX_DECLARE_EXCEPTION(TypeError, ex::Exception);
    EX_DECLARE_EXCEPTION(VersionError, ex::Exception);
}

#endif // OORT_EXCEPTION_H
