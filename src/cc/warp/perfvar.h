//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-01-15
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef WARP_PERFVAR_H
#define WARP_PERFVAR_H

#include <warp/perfitem.h>
#include <ex/exception.h>
#include <sstream>

namespace warp
{
    template <class T>
    class PerformanceVariable;

    typedef PerformanceVariable<int64_t> PerformanceInteger;
    typedef PerformanceVariable<double>  PerformanceReal;
}


//----------------------------------------------------------------------------
// PerformanceVariable
//----------------------------------------------------------------------------
template <class T>
class warp::PerformanceVariable : public warp::PerformanceItem
{
public:
    typedef T value_t;
    typedef PerformanceVariable<T> my_t;

private:
    // This should be set before registering with tracker
    std::string label;
    
    // These may be updated in a non thread-safe manner
    value_t currentValue;
    value_t reportedValue;

public:
    explicit PerformanceVariable(value_t initialValue=value_t()) :
        currentValue(initialValue) {}

    explicit PerformanceVariable(std::string const & label, value_t initialValue=value_t()) :
        label(label), currentValue(initialValue) {}

    virtual ~PerformanceVariable()
    {
        // Not strictly necessary, since PerformanceItem will remove
        // itself in its destructor, but this gives us a last chance
        // for a final log output while we still have our vtable.
        removeSelf();
    }

    /// Set variable label.  Label should be set before adding the
    /// variable to a performance tracker.  Label cannot be changed if
    /// the variable is currently assigned to a tracker.
    void setLabel(std::string const & s)
    {
        // For thread safety, don't allow modification after
        // registering with tracker
        if(this->isTracked())
        {
            using namespace ex;
            raise<RuntimeError>("cannot change the label of a tracked variable");
        }

        label = s;
    }

    /// Get current label
    std::string const & getLabel() const { return label; }

    /// Get value of tracked value
    value_t getValue() const { return currentValue; }

    /// Set value of tracked variable
    void setValue(value_t value) { currentValue = value; }

    // Convenience operators
    my_t const & operator=(value_t n)
    {
        currentValue = n;
        return *this;
    }
    my_t const & operator+=(value_t n)
    {
        currentValue += n;
        return *this;
    }

    // PerformanceItem implementation
    virtual char const * getType() const { return "VAR"; }
    virtual bool getMessage(std::string & msg, PerformanceInfo const & info)
    {
        // using boost::format;
        // using std::cerr;
        // cerr << format("update: %d (%d) %s %s f=%d\n") % info.currentTimeNs
        //     % info.deltaTimeNs % currentValue % label % info.flags;

        typedef PerformanceInfo PI;

        if(info.flags & PI::EVENT_FIRST || reportedValue != currentValue)
        {
            std::stringstream oss;
            oss << currentValue << ' ' << label;
            msg = oss.str();
            reportedValue = currentValue;
            return true;
        }

        return false;
    }
};


#endif // WARP_PERFVAR_H
