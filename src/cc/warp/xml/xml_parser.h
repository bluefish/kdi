//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-20
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

#ifndef WARP_XML_XML_PARSER_H
#define WARP_XML_XML_PARSER_H

#include <warp/file.h>
#include <warp/strutil.h>
#include <warp/hashmap.h>
#include <warp/strhash.h>
#include <ex/exception.h>

namespace warp {
namespace xml {

    /// A basic XML parser wrapped over the Expat library.
    class XmlParser;

    EX_DECLARE_EXCEPTION(XmlError, ex::RuntimeError);

} // namespace xml
} // namespace warp

//----------------------------------------------------------------------------
// XmlParser
//----------------------------------------------------------------------------
class warp::xml::XmlParser
{
public:
    typedef HashMap<StringRange, StringRange, HsiehHash> AttrMap;

public:
    XmlParser();
    virtual ~XmlParser();

    void parse(FilePtr const & f, bool clean=false);

    virtual void startElement(strref_t name, AttrMap const & attrs) {}
    virtual void endElement(strref_t name) {}
    virtual void characterData(strref_t data) {}
};


#endif // WARP_XML_XML_PARSER_H
