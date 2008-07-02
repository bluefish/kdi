//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/varsub.cc#1 $
//
// Created 2007/06/14
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "varsub.h"
#include "algorithm.h"

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
