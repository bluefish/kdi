//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-04-21
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
#include <warp/html_tokenizer.h>
#include <iostream>
#include <assert.h>

using std::cout;
using namespace warp;

//----------------------------------------------------------------------------
// Scanning utils
//----------------------------------------------------------------------------
namespace
{
    inline char const * scanIdentifier(char const * begin, char const * end)
    {
        assert(begin <= end);
        if(begin != end && isalpha(*begin))
        {
            for(++begin; begin != end; ++begin)
                if(!isalnum(*begin) && *begin != '-' && *begin != '_'
                   && *begin != ':')
                    break;
        }
        return begin;
    }

    inline char const * scanValue(char const * begin, char const * end)
    {
        assert(begin <= end);
        for(; begin != end; ++begin)
        {
            switch(*begin)
            {
                // Called for by HTML spec
                case '-':
                case '.':
                case ':':
                case '_':
                    break;

                    // Bogus but common usage
                case '#':
                case '%':
                case '&':
                case '+':
                case '/':
                case ';':
                case '=':
                case '?':
                    break;

                default:
                    if(!isalnum(*begin))
                        return begin;
            }
        }
        return begin;
    }

    inline char const * scanQuote(char const * begin, char const * end)
    {
        assert(begin <= end);
        if(begin == end)
            return end;
        else
            return std::find(begin+1, end, *begin);
    }

    inline char const * scanSpace(char const * begin, char const * end)
    {
        assert(begin <= end);
        for(; begin != end; ++begin)
            if(!isspace(*begin))
                break;
        return begin;
    }

    inline char const * scanCommentEnd(char const * begin, char const * end)
    {
        assert(begin <= end);
        while(begin != end)
        {
            if(begin[0] == '-' && end - begin > 2 && begin[1] == '-')
            {
                begin = scanSpace(begin+2, end);
                if(begin != end && *begin == '>')
                    return ++begin;
            }
            else
                ++begin;
        }
        return begin;
    }
}

//----------------------------------------------------------------------------
// HtmlTokenizer
//----------------------------------------------------------------------------
bool HtmlTokenizer::parseText(HtmlToken & x)
{
    // Parse as text node
    char const * p = ptr;
    while(++p != end && *p != '<')
        ;
            
    using namespace NodeType;
    x.setNodeType(NODE_TEXT, ptr, p);
    ptr = p;
    return true;
}

bool HtmlTokenizer::parseComment(HtmlToken & x)
{
    using namespace NodeType;
    // Comment format:
    //  <!--(stuff)*--(whitespace)*>

    // This routine is called when ptr[:4] == "<!--"
    char const * p = scanCommentEnd(ptr + 4, end);

    x.setNodeType(NODE_COMMENT, ptr, p);
    ptr = p;
    return true;
}

bool HtmlTokenizer::parseDeclaration(HtmlToken & x)
{
    using namespace NodeType;
    // Declaration format:
    //  <!(name)((whitespace)*(stuff)*)*>

    // This routine is called when
    //   ptr[:2] == "<!" and ptr[:4] != "<!--"
    char const * p = ptr + 2;

    char const * nameBegin = p;
    char const * nameEnd = scanIdentifier(nameBegin, end);
            
    if(nameBegin == nameEnd)
        return parseText(x);

    p = nameEnd;
    for(;;)
    {
        p = scanSpace(p, end);

        if(p == end)
            goto error;

        switch(*p)
        {
            case '>':
                // Done and done
                ++p;
                x.setNodeType(NODE_DECLARATION, ptr, p);
                x.setNodeName(nameBegin, nameEnd);
                ptr = p;
                return true;

            case '\'':
            case '"':
                if((p = scanQuote(p, end)) != end)
                    ++p;
                else
                    goto error;
                break;

            default:
            {
                char const * orig = p;
                p = scanIdentifier(p, end);
                if(p == orig)
                    goto error;
                break;
            }
        }
    }

  error:
    char const * pp = std::find(ptr, p, '>');
    if(pp != p)
    {
        ++pp;
        x.setNodeType(NODE_DECLARATION, ptr, pp);
        x.setNodeName(nameBegin, nameEnd);
        ptr = pp;
        return true;
    }

    return parseText(x);
}

bool HtmlTokenizer::parseTag(HtmlToken & x)
{
    /// @todo Allow tags of the form < TAG> (leading
    /// whitespace)

    /// @todo Allow tags that don't have spaces between quoted
    /// attributes: <tag a="foo"b="bar">

    /// @todo Be more forgiving on attribute parsing: <td
    /// height="14" background="/images/sf_barra.jpg"
    /// class="TESTO10" images /sf_barra.jpg>

    /// @todo More forgiving: <a onClick=someCrap('blah')>

    using namespace NodeType;
    // Declaration format:
    //  </?{id}(\s+{qid}(\s*=\s*{qid})?)*\s*/?>

    // This routine is called when
    //   ptr[:1] == "<" and ptr[:2] != "<!"
    char const * p = ptr + 1;

    bool isBegin = false;
    bool isEnd = false;
    bool haveKey = false;
    bool needValue = false;

    if(p != end && *p == '/')
    {
        ++p;
        isEnd = true;
    }
    else
        isBegin = true;

    char const * nameBegin = p;
    char const * nameEnd = scanIdentifier(nameBegin, end);
            
    if(nameBegin == nameEnd)
        return parseText(x);

    p = nameEnd;
    for(;;)
    {
        char const * p0 = p;
        p = scanSpace(p, end);

        if(p == end)
            goto error;

        char const * p1 = p;
        switch(*p)
        {
            case '/':
                if(needValue)
                    // This happens when we're scanning a tag like:
                    //   <a href=/search?q=foo&l=>
                    // Of course, this is bogus HTML according
                    // to the spec, but hey, who pays any
                    // attention to the spec?
                    //
                    // So instead of thinking this is a close
                    // tag marker, treat it as a value.
                    goto thehorror;

                if(!isEnd && ++p != end && *p == '>')
                    isEnd = true;
                else
                    goto error;
                // Fall through to '>'
                            
            case '>':
                // Done and done
                ++p;
                x.setNodeType(NODE_TAG, ptr, p);
                x.setNodeName(nameBegin, nameEnd);
                x.setTagType(isBegin, isEnd);
                ptr = p;
                return true;

            case '=':
                ++p;
                if(!haveKey || needValue)
                    goto error;
                haveKey = false;
                needValue = true;
                break;

            case '\'':
            case '"':
                if((p = scanQuote(p, end)) != end)
                    ++p;
                else
                    goto error;

                if(needValue)
                {
                    needValue = false;
                    x.addValue(p1+1, p-1);
                }
                else
                {
                    if(p0 == p1)
                        goto error;

                    x.addKey(p1+1, p-1);
                    haveKey = true;
                }
                break;
          thehorror:
            default:
                if(needValue)
                {
                    p = scanValue(p, end);
                    if(p == p1)
                        goto error;

                    x.addValue(p1, p);
                    needValue = false;
                }
                else
                {
                    if(p0 == p1)
                        goto error;

                    p = scanIdentifier(p, end);
                    if(p == p1)
                        goto error;

                    x.addKey(p1, p);
                    haveKey = true;
                }
                break;
        }
    }

  error:
    char const * pp = std::find(ptr, p, '>');
    if(pp != p)
    {
        ++pp;
        if(pp[-2] == '/')
            isEnd = true;
        x.setNodeType(NODE_TAG, ptr, pp);
        x.setNodeName(nameBegin, nameEnd);
        x.setTagType(isBegin, isEnd);
        x.clipAttributes(pp);
        ptr = pp;
        return true;
    }

    x.clear();
    return parseText(x);
}

bool HtmlTokenizer::parseScriptData(HtmlToken & x)
{
    if(ptr == end)
        return false;

    /// @todo Also scan for CDATA blocks here

    char const * p = scanSpace(ptr, end);
    if(end - p >= 4 && p[0] == '<' && p[1] == '!' && p[2] == '-' && p[3] == '-')
        p = scanCommentEnd(p + 4, end);

    while(end != (p = std::find(p, end, '<')))
    {
        if(end - p >= 3 && p[1] == '/' && isalpha(p[2]))
            break;
        else
            ++p;
    }

    using namespace NodeType;
    x.setNodeType(NODE_CDATA, ptr, p);
    ptr = p;
    return true;
}

HtmlTokenizer::HtmlTokenizer() :
    begin(0), ptr(0), end(0), nextIsScript(false)
{
}

void HtmlTokenizer::setInput(void const * src, size_t len)
{
    begin = ptr = reinterpret_cast<char const *>(src);
    end = ptr + len;
    assert(begin <= end);
}

bool HtmlTokenizer::get(HtmlToken & x)
{
    assert(begin <= end);
    using namespace NodeType;

    x.clear();
    if(ptr == end)
        return false;

    char const * p = ptr;
    bool ret;

    // Get next token
    if(nextIsScript)
    {
        nextIsScript = false;
        ret = parseScriptData(x);
    }
    else if(*p == '<')
    {
        if(++p != end && *p == '!')
        {
            if(++p != end && *p == '-' &&
               ++p != end && *p == '-')
            {
                ret = parseComment(x);
            }
            else
            {
                ret = parseDeclaration(x);
            }
        }
        else
        {
            ret = parseTag(x);
        }
    }
    else
    {
        ret = parseText(x);
    }

    // Should next tag be parsed as script data?
    if(ret && x.nodeType() == NODE_TAG &&
       x.isBeginTag() && !x.isEndTag())
    {
        switch(x.nodeName().size())
        {
            case 6:
            {
                char const * s = x.nodeName().begin();
                if((  *s == 's' || *s == 'S') &&
                   (*++s == 'c' || *s == 'C') &&
                   (*++s == 'r' || *s == 'R') &&
                   (*++s == 'i' || *s == 'I') &&
                   (*++s == 'p' || *s == 'P') &&
                   (*++s == 't' || *s == 'T'))
                {
                    nextIsScript = true;
                }
                break;
            }

            case 5:
            {
                char const * s = x.nodeName().begin();
                if((  *s == 's' || *s == 'S') &&
                   (*++s == 't' || *s == 'T') &&
                   (*++s == 'y' || *s == 'Y') &&
                   (*++s == 'l' || *s == 'L') &&
                   (*++s == 'e' || *s == 'E'))
                {
                    nextIsScript = true;
                }
                break;
            }
        }
    }

    // Return
    assert(begin <= end);
    return ret;
}
