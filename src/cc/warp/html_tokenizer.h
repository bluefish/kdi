//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/html_tokenizer.h#1 $
//
// Created 2006/04/21
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_HTML_TOKENIZER_H
#define WARP_HTML_TOKENIZER_H

#include "strutil.h"

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
    str_data_t key;
    str_data_t value;

public:
    explicit TagAttribute(str_data_t const & k = str_data_t(),
                          str_data_t const & v = str_data_t()) :
        key(k), value(v)
    {
    }

    void setKey(str_data_t const & k) { key = k; }
    void setValue(str_data_t const & v) { value = v; }
    bool clip(char const * end)
    {
        if(!key || end <= key.begin())
            return true;
        else if(end < key.end())
        {
            key = str_data_t(key.begin(), end);
            value = str_data_t();
        }
        else if(value)
        {
            if(end <= value.begin())
                value = str_data_t();
            else if(end < value.end())
                value = str_data_t(value.begin(), end);
        }
        return false;
    }

    str_data_t const & getKey() const { return key; }
    str_data_t const & getValue() const { return value; }
};


//----------------------------------------------------------------------------
// HtmlToken
//----------------------------------------------------------------------------
class warp::HtmlToken
{
    str_data_t text;
    str_data_t name;
    std::vector<TagAttribute> attrs;
    int type;
    bool isBegin;
    bool isEnd;

public:
    size_t size() const { return text.size(); }
    str_data_t const & getText() const { return text; }
    str_data_t const & nodeName() const { return name; }
    int nodeType() const { return type; }

    bool isBeginTag() const { return isBegin; }
    bool isEndTag() const { return isEnd; }

    typedef std::vector<TagAttribute>::const_iterator attr_iter;
    attr_iter beginAttrs() const { return attrs.begin(); }
    attr_iter endAttrs() const { return attrs.end(); }

    attr_iter findAttr(str_data_t const & key) const
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
        return findAttr(str_data_t(key, key+strlen(key)));
    };

    void clear()
    {
        text = str_data_t();
        name = str_data_t();
        attrs.clear();
        isBegin = isEnd = false;
    }

    void setNodeType(int type, char const * begin, char const * end)
    {
        text = str_data_t(begin, end);
        this->type = type;
    }
        
    void setNodeName(char const * begin, char const * end)
    {
        name = str_data_t(begin, end);
    }

    void setTagType(bool isBegin, bool isEnd)
    {
        this->isBegin = isBegin;
        this->isEnd = isEnd;
    }

    void addKey(char const * begin, char const * end)
    {
        attrs.push_back(TagAttribute(str_data_t(begin, end)));
    }

    void addValue(char const * begin, char const * end)
    {
        attrs.back().setValue(str_data_t(begin, end));
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
