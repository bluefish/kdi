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

#include <warp/xml/xml_parser.h>
#include <warp/charmap.h>
#include <warp/strref.h>
#include <boost/noncopyable.hpp>

extern "C" {
#include <expat.h>
}

using namespace warp;
using namespace warp::xml;
using namespace ex;

namespace
{
    void startElementCb(void * me, XML_Char const * n, XML_Char const ** a)
    {
        str_data_t name(n, n + strlen(n));
        XmlParser::AttrMap attrs;
        for(size_t i = 0; a[i]; i += 2)
            attrs.set(string_wrapper(a[i]), string_wrapper(a[i+1]));
        reinterpret_cast<XmlParser *>(me)->startElement(name, attrs);
    }

    void endElementCb(void * me, XML_Char const * n)
    {
        str_data_t name(n, n + strlen(n));
        reinterpret_cast<XmlParser *>(me)->endElement(name);
    }

    void characterDataCb(void * me, XML_Char const * d, int dlen)
    {
        str_data_t data(d, d + dlen);
        reinterpret_cast<XmlParser *>(me)->characterData(data);
    }
}

//----------------------------------------------------------------------------
// AutoParser
//----------------------------------------------------------------------------
namespace
{
    class AutoParser :
        private boost::noncopyable
    {
        XML_Parser p;
    public:

        AutoParser()
        {
            p = XML_ParserCreate(0);
        }
    
        ~AutoParser()
        {
            XML_ParserFree(p);
        }

        operator XML_Parser() const
        {
            return p;
        }
    };
}


//----------------------------------------------------------------------------
// XmlParser
//----------------------------------------------------------------------------
XmlParser::XmlParser()
{
}

XmlParser::~XmlParser()
{
}

void XmlParser::parse(FilePtr const & f, bool clean)
{
    if(!f)
        raise<ValueError>("null file");

    AutoParser parser;

    XML_SetUserData(parser, this);
    XML_SetElementHandler(parser, &startElementCb, &endElementCb);
    XML_SetCharacterDataHandler(parser, &characterDataCb);

    CharMap lowAsciiFilter;
    for(int i = 0; i < 32; ++i)
        lowAsciiFilter[i] = ' ';
    lowAsciiFilter['\n'] = '\n';
    lowAsciiFilter['\t'] = '\t';
    lowAsciiFilter['\r'] = '\r';
        
    size_t const BUFFER_SZ = 32 << 10;
    for(;;)
    {
        void * buf = XML_GetBuffer(parser, BUFFER_SZ);
        if(!buf)
            raise<RuntimeError>("couldn't get parser buffer");

        size_t sz = f->read(buf, BUFFER_SZ);
            
        if(clean)
        {
            // Convert bogus characters to spaces
            char * cp = reinterpret_cast<char *>(buf);
            std::transform(cp, cp + sz, cp, lowAsciiFilter);
        }

        XML_Status status = XML_ParseBuffer(parser, sz, sz == 0);
        if(status != XML_STATUS_OK)
        {
            XML_Error err = XML_GetErrorCode(parser);
            off_t errPos = XML_GetCurrentByteIndex(parser);
            raise<XmlError>("%s#%d: %s", f->getName(), errPos,
                            XML_ErrorString(err));
        }
        if(sz == 0)
            break;
    }
}
