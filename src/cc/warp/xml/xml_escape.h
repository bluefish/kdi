//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/xml/xml_escape.h#1 $
//
// Created 2007/12/24
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_XML_XML_ESCAPE_H
#define WARP_XML_XML_ESCAPE_H

#include "warp/strref.h"
#include <iostream>
#include <algorithm>

namespace warp {
namespace xml {

    /// Wrap a string for XML-escaped output to an ostream.  Angle
    /// brackets and ampersands in the string are replaced with XML
    /// entity references in the output stream.
    class XmlEscape;

} // namespace xml
} // namespace warp

//----------------------------------------------------------------------------
// XmlEscape
//----------------------------------------------------------------------------
class warp::xml::XmlEscape :
    public warp::str_data_t
{
public:
    explicit XmlEscape(strref_t str) : str_data_t(str) {}

private:
    friend std::ostream & operator<<(std::ostream &, XmlEscape const &);

    class IsSpecialCharacter
    {
    public:
        bool operator()(char c) const
        {
            switch(c)
            {
                case '<':
                case '>':
                case '&':
                    return true;
            }
            return false;
        }
    };

    static char const * getEscape(char c)
    {
        switch(c)
        {
            case '<': return "&lt;";
            case '>': return "&gt;";
            case '&': return "&amp;";
            default:  return "";
        }
    }
};

namespace warp {
namespace xml {

    std::ostream & operator<<(std::ostream & out, XmlEscape const & x)
    {
        char const * begin = x.begin();
        char const * end = x.end();

        for(;;)
        {
            char const * p = std::find_if(
                begin, end, XmlEscape::IsSpecialCharacter());

            out.write(begin, p - begin);
            if(p == end)
                break;

            out << XmlEscape::getEscape(*p);
        
            begin = p + 1;
        }

        return out;
    }

} // namespace xml
} // namespace warp

#endif // WARP_XML_XML_ESCAPE_H
