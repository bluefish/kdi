//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-06-14
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

#include <warp/varsub.h>
#include <warp/algorithm.h>

using namespace warp;
using namespace std;

string warp::varsub(string const & pattern,
                    map<string,string> const & vars)
{
    string out;

    char const * p = pattern.c_str();
    char const * end = p + pattern.size();

    while(p != end)
    {
        // Handle normal case of copying the pattern out
        if(*p != '$')
        {
            out += *p++;
            continue;
        }

        // At a variable indicator '$', move past
        ++p;

        // End of pattern?
        if(p == end)
        {
            // Pattern ends with $.  Invalid, but assume they meant a
            // literal $.
            out += '$';
            break;
        }

        // Escaped '$' ?
        if(*p == '$')
        {
            // Pattern contains $$, output $
            out += '$';
            ++p;
            continue;
        }

        // Get the name of the variable.
        char const * nameBegin;
        char const * nameEnd;

        // Scan variable name
        if(isalpha(*p))
        {
            // Identifier rules: ALPHA ALNUM*
            nameBegin = p++;
            while(p != end && isalnum(*p)) ++p;
            nameEnd = p;
        }
        // Maybe delimited variable: $(foo)
        else if(*p == '(' || *p == '{' || *p == '[')
        {
            char endDelim = (*p == '('  ?  ')' :
                             *p == '{'  ?  '}' :
                             /*    '['  */ ']' );
            
            // Name begins after start delimiter
            nameBegin = ++p;

            // Find end delimiter
            p = find(p, end, endDelim);
            if(p == end)
            {
                // No end delimiter... invalid, but assume the '$' was
                // meant as a literal
                p = nameBegin;
                out += '$';
                out += p[-1];
                continue;
            }

            // Name ends at end delimter
            nameEnd = p;
            ++p;
        }
        else
        {
            // Some other character after the '$'... assume it was a
            // mistake and meant as a literal '$'
            out += '$';
            out += *p++;
            continue;
        }

        // We finally have a valid variable name.  Substitute a value
        // if we have one, or leave it blank if we don't.
        out += get_assoc(vars, string(nameBegin, nameEnd), "");
    }

    return out;
}
