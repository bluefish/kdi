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

#ifndef WARP_ENVIRONMENTVARIABLE_H
#define WARP_ENVIRONMENTVARIABLE_H

#include <string>
#include <iostream>
#include <boost/noncopyable.hpp>

namespace warp {

    /// Set an environment variable for the lifetime of the object.
    /// Restore the old value when the object is destroyed.
    class EnvironmentVariable;

} // namespace warp

//----------------------------------------------------------------------------
// EnvironmentVariable
//----------------------------------------------------------------------------
class warp::EnvironmentVariable
    : private boost::noncopyable
{
    std::string name;
    std::string oldValue;
    bool hadOldValue;

public:
    /// Set an environment variable for the duration of this object.
    EnvironmentVariable(std::string const & name, std::string const & value) :
        name(name)
    {
        hadOldValue = get(name, oldValue);
        set(name, value);
    }

    /// Clear an environment variable for the duration of this object.
    explicit EnvironmentVariable(std::string const & name) :
        name(name)
    {
        hadOldValue = get(name, oldValue);
        clear(name);
    }

    /// Restore environment
    ~EnvironmentVariable()
    {
        try {
            if(hadOldValue)
                set(name, oldValue);
            else
                clear(name);
        }
        catch(std::exception const & ex) {
            std::cerr << "failed to restore environment: "
                      << ex.what() << std::endl;
        }
        catch(...) {
            std::cerr << "failed to restore environment"
                      << std::endl;
        }
    }

public:
    /// Get the current value of an environment variable.  If the
    /// environment variable exists, the value is copied and the
    /// function returns true.  Otherwise, the value is left untouched
    /// and the function returns false.
    static bool get(std::string const & name, std::string & value);

    /// Set an environment variable.  Any previous value is overwritten.
    static void set(std::string const & name, std::string const & value);

    /// Clear an environment variable.
    static void clear(std::string const & name);
};

#endif // WARP_ENVIRONMENTVARIABLE_H
