//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-11-18
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

#include <warp/EnvironmentVariable.h>
#include <ex/exception.h>
#include <stdlib.h>

using namespace warp;
using namespace ex;

//----------------------------------------------------------------------------
// EnvironmentVariable
//----------------------------------------------------------------------------
bool EnvironmentVariable::get(std::string const & name, std::string & value)
{
    if(char const * v = getenv(name.c_str()))
    {
        value = v;
        return true;
    }
    else
    {
        return false;
    }
}

void EnvironmentVariable::set(std::string const & name, std::string const & value)
{
    if(0 != setenv(name.c_str(), value.c_str(), 1))
    {
        raise<RuntimeError>("setenv (%s=%s) failed: %s",
                            name, value, getStdError());
    }
}

void EnvironmentVariable::clear(std::string const & name)
{
    if(0 != unsetenv(name.c_str()))
    {
        raise<RuntimeError>("clearenv (%s) failed: %s",
                            name, getStdError());
    }
}
