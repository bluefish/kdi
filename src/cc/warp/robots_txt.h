//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-01-20
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

#ifndef WARP_ROBOTS_TXT_H
#define WARP_ROBOTS_TXT_H

#include <warp/line_iterator.h>
#include <warp/strutil.h>
#include <warp/strref.h>
#include <string>
#include <vector>
#include <utility>
#include <algorithm>

namespace warp
{
    /// Exposes an iterable interface for parsing a robots.txt
    /// specification.
    class RobotsTxtIterator;

    /// Classification filter for URL paths based on robots.txt rules.
    class RobotsFilter;
}

//----------------------------------------------------------------------------
// RobotsTxtIterator
//----------------------------------------------------------------------------
class warp::RobotsTxtIterator
{
    LineIterator lines;

public:
    /// Initialize a RobotsTxtIterator from a character range.
    RobotsTxtIterator(char const * begin, char const * end) :
        lines(begin, end) {}

    /// Initialize a RobotsTxtIterator from a pointer and length.
    RobotsTxtIterator(char const * begin, size_t len) :
        lines(begin, len) {}

    /// Parsed types returned by get().  KEY_VALUE indicates that a
    /// key-value pair has been found.  BLANK signals that a
    /// non-comment blank line has been found.  END is used to signal
    /// the end of the buffer.
    enum LineType { KEY_VALUE, BLANK, END };

    /// Get the next parsed token in the sequence.  The token type is
    /// given by the function return value.  If the return type is
    /// KEY_VALUE, the value will be stored in the passed-in string
    /// pair.  The key is always converted to lowercase by the parser.
    LineType get(std::pair<std::string, std::string> & kv)
    {
        for(;;)
        {
            str_data_t line;
            if(!lines.get(line, true))
                return END;

            // Find first non-whitespace character
            char const * p = skipSpace(line.begin(), line.end());
            if(p == line.end())
            {
                // Blank line
                return BLANK;
            }
            else if(*p == '#')
            {
                // Comment line -- ignore
                continue;
            }

            // Key:value line

            // Effective end of line is where the comment begins
            char const * end = std::find(p, line.end(), '#');

            // Find key/value separator
            char const * sep = std::find(p, end, ':');
            if(sep == end)
            {
                // Malformed line -- missing separator
                continue;
            }

            // Get key and lowercase it
            kv.first.assign(p, skipTrailingSpace(p, sep));
            lower_inplace(kv.first);

            // Get value
            p = skipSpace(sep + 1, end);
            kv.second.assign(p, skipTrailingSpace(p, end));

            return KEY_VALUE;
        }
    }
};


//----------------------------------------------------------------------------
// RobotsFilter
//----------------------------------------------------------------------------
class warp::RobotsFilter
{
    enum Permission { ALLOWED, DISALLOWED };
    typedef std::vector< std::pair<std::string, Permission> > rulevec_t;

    std::string agent;
    rulevec_t rules;

    void addRule(std::string const & prefix, Permission perm)
    {
        rules.push_back(std::make_pair(prefix, perm));
    }

public:
    explicit RobotsFilter(std::string const & agent) :
        agent(agent) {}

    /// Decide if \c uriPath is allowed according to the currently
    /// loaded access rules.  Note that \c uriPath should only include
    /// the path and query sections of the full URI (the parts that
    /// would be sent to the HTTP server).
    bool isPathAllowed(strref_t uriPath) const;

    /// Load access rules from robots.txt data.  Only the rules that
    /// apply to our agent will be loaded.
    void loadRobotsTxt(strref_t text);
};


#endif // WARP_ROBOTS_TXT_H
