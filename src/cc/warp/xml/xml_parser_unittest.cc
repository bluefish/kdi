//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-20
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

#include "xml_parser.h"
#include "warp/memfile.h"
#include "unittest/main.h"
#include "warp/filestream.h"
#include <boost/format.hpp>
#include <string>
#include <algorithm>

using namespace warp;
using namespace warp::xml;
using namespace std;
using boost::format;

namespace
{
    string readAll(string const & fn)
    {
        FilePtr fp = File::input(fn);
        string r;
        char buf[32 << 10];
        size_t sz;
        while((sz = fp->read(buf, sizeof(buf))))
        {
            r.append(buf, buf+sz);
        }
        return r;
    }

    class PrintParser : public XmlParser
    {
        FileStream out;
        vector<char> buffer;

    public:
        explicit PrintParser(FilePtr const & out) : out(out) {}
        ~PrintParser() { flush(); }

        void flush()
        {
            if(!buffer.empty())
            {
                out << format("data %s")
                    % reprString(&buffer[0], buffer.size())
                    << endl;
                buffer.clear();
            }
        }

        void startElement(str_data_t const & name, AttrMap const & attrs)
        {
            flush();

            out << format("start %s")
                % reprString(name)
                << endl;

            vector<str_data_t> keys = attrs.getKeys();
            std::sort(keys.begin(), keys.end());
            for(vector<str_data_t>::const_iterator ki = keys.begin();
                ki != keys.end(); ++ki)
            {
                out << format("attr %s = %s")
                    % *ki
                    % reprString(attrs.get(*ki))
                    << endl;
            }
        }

        void endElement(str_data_t const & name)
        {
            flush();

            out << format("end %s")
                % reprString(name)
                << endl;
        }

        void characterData(str_data_t const & data)
        {
            buffer.insert(buffer.end(), data.begin(), data.end());
        }
    };
}


BOOST_AUTO_UNIT_TEST(test1)
{
    PrintParser(File::output("memfs:test1.out")).parse(
        makeMemFile(
            "<?xml version=\"1.0\"?>"
            "<data>"
            "<item foo=\"bar\"/>"
            "<item foo=\"baz\">"
            "hi there"
            "</item>"
            "</data>"
            )
        );

    BOOST_CHECK_EQUAL(
        readAll("memfs:test1.out"),
        "start \"data\"\n"
        "start \"item\"\n"
        "attr foo = \"bar\"\n"
        "end \"item\"\n"
        "start \"item\"\n"
        "attr foo = \"baz\"\n"
        "data \"hi there\"\n"
        "end \"item\"\n"
        "end \"data\"\n"
        );
}
