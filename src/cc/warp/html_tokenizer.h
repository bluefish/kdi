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

#ifndef WARP_HTML_TOKENIZER_H
#define WARP_HTML_TOKENIZER_H

#include <warp/strutil.h>

namespace warp
{
    namespace NodeType
    {
        enum {
            NODE_TAG,
            NODE_TEXT,
            NODE_COMMENT,
            NODE_DECLARATION,
            NODE_CDATA,
        };
    }

    class TagAttribute;
    class HtmlToken;
    class HtmlTokenizer;
};


//----------------------------------------------------------------------------
// TagAttribute
//----------------------------------------------------------------------------
class warp::TagAttribute
{
    StringRange key;
    StringRange value;

public:
    explicit TagAttribute(strref_t k = StringRange(),
                          strref_t v = StringRange()) :
        key(k), value(v)
    {
    }

    void setKey(strref_t k) { key = k; }
    void setValue(strref_t v) { value = v; }
    bool clip(char const * end)
    {
        if(!key || end <= key.begin())
            return true;
        else if(end < key.end())
        {
            key = StringRange(key.begin(), end);
            value = StringRange();
        }
        else if(value)
        {
            if(end <= value.begin())
                value = StringRange();
            else if(end < value.end())
                value = StringRange(value.begin(), end);
        }
        return false;
    }

    strref_t getKey() const { return key; }
    strref_t getValue() const { return value; }
};


//----------------------------------------------------------------------------
// HtmlToken
//----------------------------------------------------------------------------
class warp::HtmlToken
{
    StringRange text;
    StringRange name;
    std::vector<TagAttribute> attrs;
    int type;
    bool isBegin;
    bool isEnd;

public:
    size_t size() const { return text.size(); }
    strref_t getText() const { return text; }
    strref_t nodeName() const { return name; }
    int nodeType() const { return type; }

    bool isBeginTag() const { return isBegin; }
    bool isEndTag() const { return isEnd; }

    typedef std::vector<TagAttribute>::const_iterator attr_iter;
    attr_iter beginAttrs() const { return attrs.begin(); }
    attr_iter endAttrs() const { return attrs.end(); }

    attr_iter findAttr(strref_t key) const
    {
        for(attr_iter ai = beginAttrs(); ai != endAttrs(); ++ai)
        {
            if(ai->getKey() == key)
                return ai;
        }
        return endAttrs();
    }

    attr_iter findAttr(char const * key) const
    {
        return findAttr(StringRange(key, key+strlen(key)));
    };

    void clear()
    {
        text = StringRange();
        name = StringRange();
        attrs.clear();
        isBegin = isEnd = false;
    }

    void setNodeType(int type, char const * begin, char const * end)
    {
        text = StringRange(begin, end);
        this->type = type;
    }
        
    void setNodeName(char const * begin, char const * end)
    {
        name = StringRange(begin, end);
    }

    void setTagType(bool isBegin, bool isEnd)
    {
        this->isBegin = isBegin;
        this->isEnd = isEnd;
    }

    void addKey(char const * begin, char const * end)
    {
        attrs.push_back(TagAttribute(StringRange(begin, end)));
    }

    void addValue(char const * begin, char const * end)
    {
        attrs.back().setValue(StringRange(begin, end));
    }

    void clipAttributes(char const * end)
    {
        std::vector<TagAttribute>::iterator ii;
        for(ii = attrs.begin(); ii != attrs.end(); ++ii)
        {
            if(ii->clip(end))
                break;
        }
        attrs.erase(ii, attrs.end());
    }
};


//----------------------------------------------------------------------------
// HtmlTokenizer
//----------------------------------------------------------------------------
class warp::HtmlTokenizer
{
    char const * begin;
    char const * ptr;
    char const * end;
    bool nextIsScript;

    bool parseText(HtmlToken & x);
    bool parseComment(HtmlToken & x);
    bool parseDeclaration(HtmlToken & x);
    bool parseTag(HtmlToken & x);
    bool parseScriptData(HtmlToken & x);

public:
    HtmlTokenizer();

    void setInput(void const * src, size_t len);

    char const * position() const { return ptr; }
    size_t remaining() const { return end - ptr; }

    bool get(HtmlToken & x);
};


#endif // WARP_HTML_TOKENIZER_H
