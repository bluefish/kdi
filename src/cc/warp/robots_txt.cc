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

#include <warp/robots_txt.h>
#include <boost/algorithm/string.hpp>

using namespace warp;
using namespace std;
using namespace boost::algorithm;

//----------------------------------------------------------------------------
// RobotsFilter
//----------------------------------------------------------------------------
bool RobotsFilter::isPathAllowed(strref_t uriPath) const
{
    // It would be faster to use a prefix tree for this, but this
    // will work...

    Permission perm = ALLOWED;
    for(rulevec_t::const_iterator i = rules.begin();
        i != rules.end(); ++i)
    {
        if(starts_with(uriPath, i->first))
            perm = i->second;
    }

    return perm == ALLOWED;
}

void RobotsFilter::loadRobotsTxt(strref_t text)
{
    // Reset the rule set
    rules.clear();

    // We're only interested in rules that apply to our agent
    bool gatherRules = false;

    // Iterate over robots.txt keys looking for applicable rules
    RobotsTxtIterator keys(text.begin(), text.end());
    pair<string, string> kv;
    RobotsTxtIterator::LineType type = RobotsTxtIterator::END;
    while((type = keys.get(kv)) != RobotsTxtIterator::END)
    {
        if(type == RobotsTxtIterator::BLANK)
        {
            // Blank lines indicate new records
            gatherRules = false;
            continue;
        }

        // Only interested in KEY_VALUE pairs after this point
        if(type != RobotsTxtIterator::KEY_VALUE)
            continue;

        // If this is a user-agent directive, see if it matches
        // our agent
        if(kv.first == "user-agent")
        {
            // Check for wildcard match
            if(kv.second == "*" || iequals(kv.second, agent))
            {
                // Gather new a new rule set
                gatherRules = true;
                rules.clear();
            }
        }

        // We're not interested in directives unless they apply to
        // our crawl agent
        if(!gatherRules)
            continue;

        // Process access rules
        if(kv.first == "disallow")
        {
            // The special form "Disallow:" with an empty value
            // means allow everything
            if(kv.second.empty())
                addRule(kv.second, ALLOWED);
            else
                addRule(kv.second, DISALLOWED);
        }
        else if(kv.first == "allow")
            addRule(kv.second, ALLOWED);
    }

    // Sort rules
    std::sort(rules.begin(), rules.end());
}
