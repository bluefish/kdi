//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-05-14
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

#include <unittest/main.h>
#include <ex/exception.h>
#include <warp/config.h>
#include <warp/file.h>
#include <warp/fs.h>

#include <boost/test/output_test_stream.hpp>

using namespace warp;
using namespace ex;

namespace
{
    class Setup
    {
        /// Copy a string to a file
        static void setFile(std::string const & fn,
                            std::string const & contents)
        {
            FilePtr fp = File::output(fn);
            fp->write(contents.c_str(), contents.size());
            fp->close();
        }

    public:
        Setup()
        {
            setFile("memfs:/basic.cfg",
                    "# Comments should be ignored\n"
                    "  \n\n\t\n" // whitespace too
                    "  \n\n\t\n" // whitespace too
                    "! This is also a properties comment\n"
                    "   # This too is a comment\n"
                    "   ! Also a comment\n"
                    "a=1\n"
                    "b = 2\n"
                    "  c  =  3  \n"
                    "d:4\n"
                    "e : 5\n"
                    "  f  :  6  \n"
                    "g 7\n"
                    "  h   8  \n"
                    "i\n"
                    "   j     "
                );

            setFile("memfs:/crlf.cfg",
                    "# Comments should be ignored\r\n"
                    "  \r\n\r\n\t\r\n" // whitespace too
                    "  \r\n\r\n\t\r\n" // whitespace too
                    "! This is also a properties comment\r\n"
                    "   # This too is a comment\r\n"
                    "   ! Also a comment\r\n"
                    "a=1\r\n"
                    "b = 2\r\n"
                    "  c  =  3  \r\n"
                    "d:4\r\n"
                    "e : 5\r\n"
                    "  f  :  6  \r\n"
                    "g 7\r\n"
                    "  h   8  \r\n"
                    "i\r\n"
                    "   j     "
                );

            setFile("memfs:/multiline.cfg",
                    "a   =  hello \\\n"
                    "       there \\\n"
                    "       world   \n"
                    "b   : spa\\\n"
                    "tu\\\n"
                    "      la\n"
                    "c d \\ e \\\n"
                    "f g\n"
                    "h  = # Not a comment \\\n"
                    "! Not a comment\n"
                    "\n"
                    "g\\\n"
                    "\\\n"
                    "     \\\n"
                    "waka  \n"
                    "j : string with a backslash at the end\\\\\n"
                    "k : broken multiline\\\n"
                );

            setFile("memfs:/hierarchy.cfg",
                    "A      = A\n"
                    "A.a.i  = Aai\n"
                    "A.a.ii = Aaii\n"
                    "A.b.i  = Abi\n"
                    "B.a.i  = Bai\n"
                    "B.b.i  = Bbi\n"
                    "A.a    = Aa\n"
                );

            setFile("memfs:/escape.cfg",
                    "a : trailing space\\ \n"
                    "b : embedded \\n newline\n"
                    "c : hex\\x20decoding\n"
                    "d : unicode\\u0020decoding\n"
                    "e : unicode\\U00000020decoding\n"
                    "f : backslash \\\\\n"
                    "g \\:\n"
                    "h \\=\n"
                    "i : special escapes \\a\\t\\n\\f\\r\\v\\\\\n"
                    "j : \\i\\gn\\or\\e\\d\\ \\e\\s\\ca\\p\\e\\s\n"
                    "k : embedded \\x00 null\n"
                );

            fs::makedirs("memfs:/nested/path");
            setFile("memfs:/nested/path/foo.cfg",
                    "some.file = foo.txt\n"
                    "this.dir = .\n"
                    "parent.dir = ..\n"
                    "absolute = /waka/fuzz.html\n"
                    "web = http://www.kosmix.com/\n"
                );
        }
    };
}

BOOST_AUTO_TEST_CASE(basic)
{
    Config cfg;

    // Config should be empty
    BOOST_CHECK_EQUAL(cfg.numChildren(), 0u);

    // Set a value and make sure we can read it back
    cfg.set("foo", "bar");
    BOOST_CHECK_EQUAL(cfg.get("foo"), "bar");
    BOOST_CHECK_EQUAL(cfg.getChild(0).get(), "bar");
    BOOST_CHECK_EQUAL(cfg.numChildren(), 1u);

    // Ensure that overwrite works
    cfg.set("foo", "waka");
    BOOST_CHECK_EQUAL(cfg.get("foo"), "waka");
    BOOST_CHECK_EQUAL(cfg.numChildren(), 1u);

    // Try a hierarchical set
    cfg.set("test.one.two", "spam");
    BOOST_CHECK_EQUAL(cfg.get("foo"), "waka");
    BOOST_CHECK_EQUAL(cfg.get("test.one.two"), "spam");
    BOOST_CHECK_EQUAL(cfg.numChildren(), 2u);
}

BOOST_AUTO_TEST_CASE(ordering)
{
    // Keys should be indexed in order of first assignment

    Config cfg;

    cfg.set("a", "one");
    cfg.set("b", "two");
    cfg.set("c", "three");

    BOOST_CHECK_EQUAL(cfg.numChildren(), 3u);

    BOOST_CHECK_EQUAL(cfg.getChild(0).get(), "one");
    BOOST_CHECK_EQUAL(cfg.getChild(1).get(), "two");
    BOOST_CHECK_EQUAL(cfg.getChild(2).get(), "three");

    BOOST_CHECK_EQUAL(cfg.getChild(0).getKey(), "a");
    BOOST_CHECK_EQUAL(cfg.getChild(1).getKey(), "b");
    BOOST_CHECK_EQUAL(cfg.getChild(2).getKey(), "c");
    
    cfg.set("b", "four");
    cfg.set("a", "five");

    BOOST_CHECK_EQUAL(cfg.numChildren(), 3u);

    BOOST_CHECK_EQUAL(cfg.getChild(0).get(), "five");
    BOOST_CHECK_EQUAL(cfg.getChild(1).get(), "four");
    BOOST_CHECK_EQUAL(cfg.getChild(2).get(), "three");

    BOOST_CHECK_EQUAL(cfg.getChild(0).getKey(), "a");
    BOOST_CHECK_EQUAL(cfg.getChild(1).getKey(), "b");
    BOOST_CHECK_EQUAL(cfg.getChild(2).getKey(), "c");
}

BOOST_AUTO_TEST_CASE(copy)
{
    // We should be able to do a deep copy and have independent config
    // trees

    // Make original
    Config a;
    a.set("test.one", "stuff");
    a.set("test.two", "things");

    // Copy constructor
    Config b(a);

    // Assignment
    Config c;
    c.set("overwrite.me", "bye!");
    c = a;


    // All three should be equivalent to A now
    
    BOOST_CHECK_EQUAL(a.get("test.one"), "stuff");
    BOOST_CHECK_EQUAL(a.get("test.two"), "things");

    BOOST_CHECK_EQUAL(b.get("test.one"), "stuff");
    BOOST_CHECK_EQUAL(b.get("test.two"), "things");

    BOOST_CHECK_EQUAL(c.get("test.one"), "stuff");
    BOOST_CHECK_EQUAL(c.get("test.two"), "things");


    // C should have lost its former key

    BOOST_CHECK_THROW(c.get("overwrite.me"), KeyError);


    // Mutate configs and make sure they are different

    a.set("test.one", "A");
    b.set("test.one", "B");
    c.set("test.one", "C");

    BOOST_CHECK_EQUAL(a.get("test.one"), "A");
    BOOST_CHECK_EQUAL(a.get("test.two"), "things");

    BOOST_CHECK_EQUAL(b.get("test.one"), "B");
    BOOST_CHECK_EQUAL(b.get("test.two"), "things");

    BOOST_CHECK_EQUAL(c.get("test.one"), "C");
    BOOST_CHECK_EQUAL(c.get("test.two"), "things");
}


BOOST_AUTO_TEST_CASE(exceptions)
{
    // Make sure we throw exceptions for invalid key references

    // Valid keys are dotted sequences of identifiers, where an
    // identifier starts with an alpha character or an underscore, and
    // is followed by zero or more alpha-numeric characters or
    // underscores.

    Config cfg;

    // get methods also require that the key exists

    BOOST_CHECK_THROW(cfg.get("dne"), KeyError);
    BOOST_CHECK_THROW(cfg.get("does.not.exist"), KeyError);
    BOOST_CHECK_THROW(cfg.get("invalid-key"), ValueError);

    BOOST_CHECK_THROW(cfg.get(""), ValueError);
    BOOST_CHECK_THROW(cfg.get("."), ValueError);
    BOOST_CHECK_THROW(cfg.get(".no.predot"), ValueError);
    BOOST_CHECK_THROW(cfg.get("no.postdot."), ValueError);
    BOOST_CHECK_THROW(cfg.get("no..empty.segments"), ValueError);

    BOOST_CHECK_THROW(cfg.getChild(6).get(), IndexError);
}

BOOST_AUTO_TEST_CASE(prop_basic)
{
    Setup fixture;

    // Test comments and all the basic assignment forms
    Config cfg("memfs:/basic.cfg");

    BOOST_CHECK_EQUAL(cfg.get("a"), "1");
    BOOST_CHECK_EQUAL(cfg.get("b"), "2");
    BOOST_CHECK_EQUAL(cfg.get("c"), "3  ");
    BOOST_CHECK_EQUAL(cfg.get("d"), "4");
    BOOST_CHECK_EQUAL(cfg.get("e"), "5");
    BOOST_CHECK_EQUAL(cfg.get("f"), "6  ");
    BOOST_CHECK_EQUAL(cfg.get("g"), "7");
    BOOST_CHECK_EQUAL(cfg.get("h"), "8  ");
    BOOST_CHECK_EQUAL(cfg.get("i"), "");
    BOOST_CHECK_EQUAL(cfg.get("j"), "");
    BOOST_CHECK_EQUAL(cfg.numChildren(), 10u);
}

BOOST_AUTO_TEST_CASE(prop_crlf)
{
    Setup fixture;

    // Test comments and all the basic assignment forms
    Config cfg("memfs:/crlf.cfg");

    BOOST_CHECK_EQUAL(cfg.get("a"), "1");
    BOOST_CHECK_EQUAL(cfg.get("b"), "2");
    BOOST_CHECK_EQUAL(cfg.get("c"), "3  ");
    BOOST_CHECK_EQUAL(cfg.get("d"), "4");
    BOOST_CHECK_EQUAL(cfg.get("e"), "5");
    BOOST_CHECK_EQUAL(cfg.get("f"), "6  ");
    BOOST_CHECK_EQUAL(cfg.get("g"), "7");
    BOOST_CHECK_EQUAL(cfg.get("h"), "8  ");
    BOOST_CHECK_EQUAL(cfg.get("i"), "");
    BOOST_CHECK_EQUAL(cfg.get("j"), "");
    BOOST_CHECK_EQUAL(cfg.numChildren(), 10u);
}

BOOST_AUTO_TEST_CASE(prop_multiline)
{
    Setup fixture;

    // Test multi-line continuations
    Config cfg("memfs:/multiline.cfg");

    BOOST_CHECK_EQUAL(cfg.get("a"), "hello there world   ");
    BOOST_CHECK_EQUAL(cfg.get("b"), "spatula");
    BOOST_CHECK_EQUAL(cfg.get("c"), "d  e f g");
    BOOST_CHECK_EQUAL(cfg.get("h"), "# Not a comment ! Not a comment");
    BOOST_CHECK_EQUAL(cfg.get("g"), "waka  ");
    BOOST_CHECK_EQUAL(cfg.get("j"), "string with a backslash at the end\\");
    BOOST_CHECK_EQUAL(cfg.get("k"), "broken multiline");
    BOOST_CHECK_EQUAL(cfg.numChildren(), 7u);
}

BOOST_AUTO_TEST_CASE(prop_hierarchical)
{
    Setup fixture;

    // Test heirarchical assignment
    Config cfg("memfs:/hierarchy.cfg");

    BOOST_CHECK_EQUAL(cfg.get("A"), "A");
    BOOST_CHECK_EQUAL(cfg.get("A.a"), "Aa");
    BOOST_CHECK_EQUAL(cfg.get("A.a.i"), "Aai");
    BOOST_CHECK_EQUAL(cfg.get("A.a.ii"), "Aaii");
    BOOST_CHECK_EQUAL(cfg.get("A.b.i"), "Abi");
    BOOST_CHECK_EQUAL(cfg.get("B.a.i"), "Bai");
    BOOST_CHECK_EQUAL(cfg.get("B.b.i"), "Bbi");
    BOOST_CHECK_EQUAL(cfg.numChildren(), 2u);
    BOOST_CHECK_EQUAL(cfg.getChild("A").numChildren(), 2u);
    BOOST_CHECK_EQUAL(cfg.getChild("B").numChildren(), 2u);
    BOOST_CHECK_EQUAL(cfg.getChild("A.a").numChildren(), 2u);
    BOOST_CHECK_EQUAL(cfg.getChild("A.b").numChildren(), 1u);
    BOOST_CHECK_EQUAL(cfg.getChild("B.a").numChildren(), 1u);
    BOOST_CHECK_EQUAL(cfg.getChild("B.b").numChildren(), 1u);
    BOOST_CHECK_EQUAL(cfg.getChild("A.a.i").numChildren(), 0u);
    BOOST_CHECK_EQUAL(cfg.getChild("A.a.ii").numChildren(), 0u);
    BOOST_CHECK_EQUAL(cfg.getChild("A.b.i").numChildren(), 0u);
    BOOST_CHECK_EQUAL(cfg.getChild("B.a.i").numChildren(), 0u);
    BOOST_CHECK_EQUAL(cfg.getChild("B.b.i").numChildren(), 0u);
}

BOOST_AUTO_TEST_CASE(prop_escape)
{
    Setup fixture;

    // Test escape sequence processing
    Config cfg("memfs:/escape.cfg");
    
    BOOST_CHECK_EQUAL(cfg.get("a"), "trailing space ");
    BOOST_CHECK_EQUAL(cfg.get("b"), "embedded \n newline");
    BOOST_CHECK_EQUAL(cfg.get("c"), "hex decoding");
    BOOST_CHECK_EQUAL(cfg.get("d"), "unicode decoding");
    BOOST_CHECK_EQUAL(cfg.get("e"), "unicode decoding");
    BOOST_CHECK_EQUAL(cfg.get("f"), "backslash \\");
    BOOST_CHECK_EQUAL(cfg.get("g"), ":");
    BOOST_CHECK_EQUAL(cfg.get("h"), "=");
    BOOST_CHECK_EQUAL(cfg.get("i"), "special escapes \a\t\n\f\r\v\\");
    BOOST_CHECK_EQUAL(cfg.get("j"), "ignored escapes");
    char const nullData[] = "embedded \0 null";
    std::string nullStr(nullData, nullData + sizeof(nullData) - 1);
    BOOST_CHECK_EQUAL(cfg.get("k"), nullStr);
}

BOOST_AUTO_TEST_CASE(cfg_reroot)
{
    Setup fixture;
    
    // Make sure URI-based config rerooting works properly
    Config cfg;

    cfg.loadProperties("memfs:/hierarchy.cfg?configRoot=A");
    BOOST_CHECK_THROW(cfg.get("A"), KeyError);
    BOOST_CHECK_EQUAL(cfg.get("a"), "Aa");
    BOOST_CHECK_EQUAL(cfg.get("a.i"), "Aai");
    BOOST_CHECK_EQUAL(cfg.get("a.ii"), "Aaii");
    BOOST_CHECK_EQUAL(cfg.get("b.i"), "Abi");
    BOOST_CHECK_EQUAL(cfg.numChildren(), 2u);

    cfg.loadProperties("memfs:/hierarchy.cfg?configRoot=B");
    BOOST_CHECK_EQUAL(cfg.get("a.i"), "Bai");
    BOOST_CHECK_EQUAL(cfg.get("b.i"), "Bbi");
    BOOST_CHECK_EQUAL(cfg.numChildren(), 2u);

    cfg.loadProperties("memfs:/hierarchy.cfg?configRoot=A.b");
    BOOST_CHECK_EQUAL(cfg.get("i"), "Abi");
    BOOST_CHECK_EQUAL(cfg.numChildren(), 1u);
}

BOOST_AUTO_TEST_CASE(cfg_uri)
{
    Setup fixture;

    // Test to see if URIs work on config nodes
    Config cfg("memfs:/hierarchy.cfg");

    BOOST_CHECK_EQUAL(cfg.getBaseUri(),         "memfs:/hierarchy.cfg");
    BOOST_CHECK_EQUAL(cfg.getBaseUri("A"),      "memfs:/hierarchy.cfg?configRoot=A");
    BOOST_CHECK_EQUAL(cfg.getBaseUri("A.a"),    "memfs:/hierarchy.cfg?configRoot=A.a");
    BOOST_CHECK_EQUAL(cfg.getBaseUri("A.a.i"),  "memfs:/hierarchy.cfg?configRoot=A.a.i");
    BOOST_CHECK_EQUAL(cfg.getBaseUri("A.a.ii"), "memfs:/hierarchy.cfg?configRoot=A.a.ii");
    BOOST_CHECK_EQUAL(cfg.getBaseUri("A.b"),    "memfs:/hierarchy.cfg?configRoot=A.b");
    BOOST_CHECK_EQUAL(cfg.getBaseUri("B"),      "memfs:/hierarchy.cfg?configRoot=B");
    BOOST_CHECK_EQUAL(cfg.getBaseUri("B.a"),    "memfs:/hierarchy.cfg?configRoot=B.a");

    cfg.loadProperties("memfs:/hierarchy.cfg?configRoot=A.a");
    BOOST_CHECK_EQUAL(cfg.getBaseUri(),         "memfs:/hierarchy.cfg?configRoot=A.a");
    BOOST_CHECK_EQUAL(cfg.getBaseUri("i"),      "memfs:/hierarchy.cfg?configRoot=A.a.i");
    BOOST_CHECK_EQUAL(cfg.getBaseUri("ii"),     "memfs:/hierarchy.cfg?configRoot=A.a.ii");
}

BOOST_AUTO_TEST_CASE(cfg_relative_uri)
{
    Setup fixture;

    Config cfg("memfs:/nested/path/foo.cfg");

    BOOST_CHECK_EQUAL(cfg.getAsUri("some.file"), "memfs:/nested/path/foo.txt");
    BOOST_CHECK_EQUAL(cfg.getAsUri("this.dir"), "memfs:/nested/path/");
    BOOST_CHECK_EQUAL(cfg.getAsUri("parent.dir"), "memfs:/nested/");
    BOOST_CHECK_EQUAL(cfg.getAsUri("absolute"), "memfs:/waka/fuzz.html");
    BOOST_CHECK_EQUAL(cfg.getAsUri("web"), "http://www.kosmix.com/");

    BOOST_CHECK_EQUAL(cfg.getAsUri("some.file", "default/badness", true), "memfs:/nested/path/foo.txt");
    BOOST_CHECK_EQUAL(cfg.getAsUri("does.not.exist", "default/goodness", true), "memfs:/nested/path/default/goodness");

    BOOST_CHECK_EQUAL(cfg.getAsUri("some.file", "default/badness", false), "memfs:/nested/path/foo.txt");
    BOOST_CHECK_EQUAL(cfg.getAsUri("does.not.exist", "default/goodness", false), "default/goodness");
}

BOOST_AUTO_TEST_CASE(cfg_full_key)
{
    Setup fixture;

    // Test to see if URIs work on config nodes
    Config cfg("memfs:/hierarchy.cfg");

    BOOST_CHECK_EQUAL(cfg.getKey(),                        "");
    BOOST_CHECK_EQUAL(cfg.getChild("A").getKey(),          "A");
    BOOST_CHECK_EQUAL(cfg.getChild("A.a").getKey(),        "a");
    BOOST_CHECK_EQUAL(cfg.getChild("A.a.i").getKey(),      "i");

    BOOST_CHECK_EQUAL(cfg.getFullKey(),                    "");
    BOOST_CHECK_EQUAL(cfg.getChild("A").getFullKey(),      "A");
    BOOST_CHECK_EQUAL(cfg.getChild("A.a").getFullKey(),    "A.a");
    BOOST_CHECK_EQUAL(cfg.getChild("A.a.i").getFullKey(),  "A.a.i");

    // Make a copy of a child
    cfg = cfg.getChild("A");

    BOOST_CHECK_EQUAL(cfg.getKey(),                        "A");
    BOOST_CHECK_EQUAL(cfg.getChild("a").getKey(),          "a");
    BOOST_CHECK_EQUAL(cfg.getChild("a.i").getKey(),        "i");

    BOOST_CHECK_EQUAL(cfg.getFullKey(),                    "A");
    BOOST_CHECK_EQUAL(cfg.getChild("a").getFullKey(),      "A.a");
    BOOST_CHECK_EQUAL(cfg.getChild("a.i").getFullKey(),    "A.a.i");

    // Reload from a rooted location
    cfg.loadProperties("memfs:/hierarchy.cfg?configRoot=A");

    BOOST_CHECK_EQUAL(cfg.getKey(),                        "A");
    BOOST_CHECK_EQUAL(cfg.getChild("a").getKey(),          "a");
    BOOST_CHECK_EQUAL(cfg.getChild("a.i").getKey(),        "i");

    BOOST_CHECK_EQUAL(cfg.getFullKey(),                    "A");
    BOOST_CHECK_EQUAL(cfg.getChild("a").getFullKey(),      "A.a");
    BOOST_CHECK_EQUAL(cfg.getChild("a.i").getFullKey(),    "A.a.i");
}

BOOST_AUTO_TEST_CASE(cfg_ostream)
{
    Config cfg;

    cfg.set("z", "foo");
    cfg.set("a.b.c.d", "bar");
    cfg.set("a.b", "  spaces  ");
    cfg.set("y", "new\nline");

    boost::test_tools::output_test_stream out;
    out << cfg;

    BOOST_CHECK(out.is_equal(
                    "z = foo\n"
                    "a.b = \\  spaces  \n"
                    "a.b.c.d = bar\n"
                    "y = new\\nline\n"
                    ));
}

BOOST_AUTO_TEST_CASE(cfg_writeProperties)
{
    // Set and write properties
    {
        Config cfg;
        
        cfg.set("z", "foo");
        cfg.set("a.b.c.d", "bar");
        cfg.set("a.b", "  spaces  ");
        cfg.set("y", "new\nline");
        
        cfg.writeProperties("memfs:out.cfg");
    }
    
    // Make sure the result is what we expect
    {
        FilePtr fp = File::input("memfs:out.cfg");
        char buf[2048];
        size_t sz = fp->read(buf, sizeof(buf));
        
        std::string result(buf, buf+sz);
        BOOST_CHECK_EQUAL(result, 
                          "z = foo\n"
                          "a.b = \\  spaces  \n"
                          "a.b.c.d = bar\n"
                          "y = new\\nline\n"
            );
    }

    // Re-read the properties and check values
    {
        Config cfg("memfs:out.cfg");

        BOOST_CHECK_EQUAL(cfg.get("z"), "foo");
        BOOST_CHECK_EQUAL(cfg.get("a.b"), "  spaces  ");
        BOOST_CHECK_EQUAL(cfg.get("a.b.c.d"), "bar");
        BOOST_CHECK_EQUAL(cfg.get("y"), "new\nline");
    }
}
