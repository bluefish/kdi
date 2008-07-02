//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/varsub.h#1 $
//
// Created 2007/06/14
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_VARSUB_H
#define WARP_VARSUB_H

#include <string>
#include <map>

namespace warp
{
    /// Substitute variables of the form "$KEY", "$(KEY)", "${KEY}",
    /// or "$[KEY]" with the value associated with "KEY" in the given
    /// variable map.  If the value is not found, substitute an empty
    /// string.  Literal dollar signs can be including using "$$".
    /// Also, if a single "$" appears with an invalid variable
    /// specification, the literal "$" will be retained.  The
    /// delimited forms of variable naming may contain any characters
    /// except for the end delimiter.  The non-delimited form must be
    /// a valid identifier: an alphabetic character followed by zero
    /// or more alpha-numeric characters.
    std::string varsub(std::string const & pattern,
                       std::map<std::string, std::string> const & vars);
}

#endif // WARP_VARSUB_H
