//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-10
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

#include "strutil.h"
#include "util.h"
#include "html_tokenizer.h"
#include "ex/exception.h"
#include <boost/format.hpp>

using namespace warp;
using namespace ex;

//----------------------------------------------------------------------------
// replace_inplace
//----------------------------------------------------------------------------
void warp::replace_inplace(std::string & s,
                          std::string const & match,
                          std::string const & repl)
{
    std::string::size_type rlen = repl.length();
    std::string::size_type mlen = match.length();
    if(mlen == 0)
        rlen += 1;
        
    for(std::string::size_type pos = s.find(match);
        pos != std::string::npos;
        pos = s.find(match, pos + rlen))
    {
        s.replace(pos, mlen, repl);
    }
}


//----------------------------------------------------------------------------
// sizeString
//----------------------------------------------------------------------------
std::string warp::sizeString(int64_t sz, int64_t suffixFactor)
{
    char const SUFFIX[] = " KMGTPE";
    int sufIdx = 0;
    while(sz >= suffixFactor * suffixFactor)
    {
        sz /= suffixFactor;
        ++sufIdx;
    }
    if(sz >= suffixFactor)
        return boost::str( boost::format("%.1f%c")
                           % (double(sz) / suffixFactor)
                           % SUFFIX[sufIdx+1] );
    else
        return boost::str( boost::format("%d%c") % sz % SUFFIX[sufIdx] );
}


//----------------------------------------------------------------------------
// parseSize
//----------------------------------------------------------------------------
size_t warp::parseSize(std::string const & sizeString)
{
    char const * p = sizeString.c_str();
    while(isspace(*p))
        ++p;
    if(!isdigit(*p))
        raise<RuntimeError>("invalid size: %s", sizeString.c_str());
    size_t n = *p - '0';
    size_t d = 1;
    for(++p; isdigit(*p); ++p)
        n = n * 10 + *p - '0';
    if(*p == '.')
        for(++p; isdigit(*p); ++p, d *= 10)
            n = n * 10 + *p - '0';
    while(isspace(*p))
        ++p;
    switch(*p)
    {
        case '\0':
            return n / d;
        case 'k': case 'K':
            n <<= 10;
            break;
        case 'm': case 'M':
            n <<= 20;
            break;
        case 'g': case 'G':
            n <<= 30;
            break;
        default:
            raise<RuntimeError>("invalid size: %s", sizeString.c_str());
    }
    if(*++p != '\0')
        raise<RuntimeError>("invalid size: %s", sizeString.c_str());

    return n / d;
}

//----------------------------------------------------------------------------
// stripHtml
//----------------------------------------------------------------------------
std::string warp::stripHtml(str_data_t const & src, bool insertSpaces)
{
    std::string output;

    HtmlTokenizer tok;
    tok.setInput(src.begin(), src.size());

    bool needSpace = false;
    HtmlToken t;
    while(tok.get(t))
    {
        if(t.nodeType() == NodeType::NODE_TEXT)
        {
            if(needSpace && insertSpaces)
                output += ' ';
            needSpace = false;
                
            output += t.getText();
        }
        else
            needSpace = true;
    }

    return output;
}

//----------------------------------------------------------------------------
// typeString
//----------------------------------------------------------------------------
std::string warp::typeString(uint32_t typecode)
{
    char const * p = (char const *)&typecode;
    return reprString(p, p + sizeof(typecode), false); 
}

//----------------------------------------------------------------------------
// reprString
//----------------------------------------------------------------------------
std::string warp::reprString(char const * begin, char const * end, bool quote)
{
    char const HEX[] = "0123456789abcdef";
    std::string r;
    if(quote)
        r += '"';
    for(char const * p = begin; p != end; ++p)
    {
        switch(*p)
        {
            case '\n': r += '\\'; r += 'n';  break;
            case '\t': r += '\\'; r += 't';  break;
            case '\v': r += '\\'; r += 'v';  break;
            case '\b': r += '\\'; r += 'b';  break;
            case '\r': r += '\\'; r += 'r';  break;
            case '\f': r += '\\'; r += 'f';  break;
            case '\a': r += '\\'; r += 'a';  break;
            case '\\': r += '\\'; r += '\\'; break;
            case '"':  r += '\\'; r += '"';  break;

            default:
                if(isprint(*p))
                    r += *p;
                else
                {
                    r += '\\'; r += 'x'; 
                    r += HEX[(*p >> 4) & 0x0f];
                    r += HEX[*p & 0x0f];
                }
        }
    }

    if(quote)
        r += '"';

    return r;
}

//----------------------------------------------------------------------------
// hexString
//----------------------------------------------------------------------------
void warp::hexString(uint8_t const * begin, uint8_t const * end,
                     char * out)
{
    char const HEX[] = "0123456789abcdef";
    for(; begin != end; ++begin)
    {
        uint8_t c = *begin;
        *out++ = HEX[(c >> 4) & 0x0f];
        *out++ = HEX[c & 0x0f];
    }
}

std::string warp::hexString(uint8_t const * begin, uint8_t const * end)
{
    std::string r;
    char const HEX[] = "0123456789abcdef";
    for(; begin != end; ++begin)
    {
        uint8_t c = *begin;
        r += HEX[(c >> 4) & 0x0f];
        r += HEX[c & 0x0f];
    }
    return r;
}

std::string warp::memDump(void const * ptr, size_t len, size_t wordSize)
{
    std::string r;
    char const HEX[] = "0123456789abcdef";

    uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
    uint8_t const * p = reinterpret_cast<uint8_t const *>(ptr);

    for(size_t i = 0; i < len; ++i, ++p, ++addr)
    {
        if(i && (addr % wordSize) == 0)
            r += ' ';

        uint8_t c = *p;
        r += HEX[(c >> 4) & 0x0f];
        r += HEX[c & 0x0f];
    }

    return r;
}

void warp::alphaNormalize(std::string & x)
{
    bool needSpace = false;
    char * o = &x[0];
    char const * i = &x[0];
    char const * end = i + x.size();

    for(; i != end; ++i)
    {
        if(isalpha(*i))
        {
            if(needSpace)
            {
                *o++ = ' ';
                needSpace = false;
            }
            *o++ = tolower(*i);
        }
        else if(o != &x[0])
            needSpace = true;
    }
    x.erase(o - &x[0]);
}
