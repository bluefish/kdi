//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/attic/client/error_buffer.h#1 $
//
// Created 2007/11/09
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_CLIENT_ERROR_BUFFER_H
#define KDI_CLIENT_ERROR_BUFFER_H

#include <vector>
#include <string>
#include "ex/exception.h"

namespace kdi {
namespace client {

    class ErrorBuffer;

} // namespace client
} // namespace kdi

//----------------------------------------------------------------------------
// ErrorBuffer
//----------------------------------------------------------------------------
class kdi::client::ErrorBuffer
{
    std::string name;
    std::vector<std::string> errors;

    struct unspecified_bool_t {};
    
public:
    /// Create an ErrorBuffer that throws with the given name.
    explicit ErrorBuffer(std::string const & name) : name(name) {}

    /// Append an error message to the error buffer
    void append(std::string const & message)
    {
        errors.push_back(message);
    }

    /// If any errors have been collected, throw a RuntimeException
    /// containing the messages from all of them.
    void maybeThrow() const
    {
        if(!errors.empty())
        {
            std::ostringstream buf;
            for(std::vector<std::string>::const_iterator i = errors.begin();
                i != errors.end(); ++i)
            {
                buf << "\n  " << *i;
            }
            size_t nErrs = errors.size();
            errors.clear();
            ex::raise<ex::RuntimeError>("%s has collected %s error(s):%s",
                                        name, nErrs, buf.str());
        }
    }

    /// Returns true iff there are errors
    operator unspecified_bool_t const *() const
    {
        if(errors.empty())
            return 0;
        else
            return reinterpret_cast<unspecified_bool_t const *>(this);
    }
};


#endif // KDI_CLIENT_ERROR_BUFFER_H
