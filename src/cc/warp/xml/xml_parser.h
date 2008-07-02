//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/xml/xml_parser.h#1 $
//
// Created 2007/12/20
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_XML_XML_PARSER_H
#define WARP_XML_XML_PARSER_H

#include "warp/file.h"
#include "warp/strutil.h"
#include "warp/hashmap.h"
#include "warp/strhash.h"
#include "ex/exception.h"

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
    typedef HashMap<str_data_t, str_data_t, HsiehHash> AttrMap;

public:
    XmlParser();
    virtual ~XmlParser();

    void parse(FilePtr const & f, bool clean=false);

    virtual void startElement(str_data_t const & name, AttrMap const & attrs) {}
    virtual void endElement(str_data_t const & name) {}
    virtual void characterData(str_data_t const & data) {}
};


#endif // WARP_XML_XML_PARSER_H
