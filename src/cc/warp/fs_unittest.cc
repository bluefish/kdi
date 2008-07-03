//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-09-25
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

#include "fs.h"
#include "unittest/main.h"

using namespace warp;
using namespace std;

BOOST_AUTO_TEST_CASE(test_parse)
{
    string p = "scheme://auth/usr/local/bin?query#fragment";

    BOOST_CHECK_EQUAL(fs::path(p), "/usr/local/bin");
    BOOST_CHECK_EQUAL(fs::basename(p), "bin");
    BOOST_CHECK_EQUAL(fs::dirname(p), "/usr/local");
    BOOST_CHECK_EQUAL(fs::extension(p), "");

    p = fs::resolve(p, "foo.ext");
    BOOST_CHECK_EQUAL(fs::path(p), "/usr/local/bin/foo.ext");
    BOOST_CHECK_EQUAL(fs::basename(p), "foo.ext");
    BOOST_CHECK_EQUAL(fs::basename(p, true), "foo");
    BOOST_CHECK_EQUAL(fs::dirname(p), "/usr/local/bin");
    BOOST_CHECK_EQUAL(fs::extension(p), ".ext");
}

BOOST_AUTO_TEST_CASE(test_resolve)
{
    // Test Unix path resolution
    BOOST_CHECK_EQUAL(fs::resolve("",    ""), "");
    BOOST_CHECK_EQUAL(fs::resolve(".",   ""), "");
    BOOST_CHECK_EQUAL(fs::resolve("./",  ""), "");
    BOOST_CHECK_EQUAL(fs::resolve("..",  ""), "../");
    BOOST_CHECK_EQUAL(fs::resolve("../", ""), "../");

    BOOST_CHECK_EQUAL(fs::resolve("",    "."), "");
    BOOST_CHECK_EQUAL(fs::resolve(".",   "."), "");
    BOOST_CHECK_EQUAL(fs::resolve("./",  "."), "");
    BOOST_CHECK_EQUAL(fs::resolve("..",  "."), "../");
    BOOST_CHECK_EQUAL(fs::resolve("../", "."), "../");

    BOOST_CHECK_EQUAL(fs::resolve("",    "./"), "");
    BOOST_CHECK_EQUAL(fs::resolve(".",   "./"), "");
    BOOST_CHECK_EQUAL(fs::resolve("./",  "./"), "");
    BOOST_CHECK_EQUAL(fs::resolve("..",  "./"), "../");
    BOOST_CHECK_EQUAL(fs::resolve("../", "./"), "../");

    BOOST_CHECK_EQUAL(fs::resolve("/",    ""), "/");
    BOOST_CHECK_EQUAL(fs::resolve("/.",   ""), "/");
    BOOST_CHECK_EQUAL(fs::resolve("/./",  ""), "/");
    BOOST_CHECK_EQUAL(fs::resolve("/..",  ""), "/");
    BOOST_CHECK_EQUAL(fs::resolve("/../", ""), "/");

    BOOST_CHECK_EQUAL(fs::resolve("",    "foo"), "foo");
    BOOST_CHECK_EQUAL(fs::resolve(".",   "foo"), "foo");
    BOOST_CHECK_EQUAL(fs::resolve("./",  "foo"), "foo");
    BOOST_CHECK_EQUAL(fs::resolve("..",  "foo"), "../foo");
    BOOST_CHECK_EQUAL(fs::resolve("../", "foo"), "../foo");

    BOOST_CHECK_EQUAL(fs::resolve("/",    "foo"), "/foo");
    BOOST_CHECK_EQUAL(fs::resolve("/.",   "foo"), "/foo");
    BOOST_CHECK_EQUAL(fs::resolve("/./",  "foo"), "/foo");
    BOOST_CHECK_EQUAL(fs::resolve("/..",  "foo"), "/foo");
    BOOST_CHECK_EQUAL(fs::resolve("/../", "foo"), "/foo");

    BOOST_CHECK_EQUAL(fs::resolve("",    ".foo"), ".foo");
    BOOST_CHECK_EQUAL(fs::resolve(".",   ".foo"), ".foo");
    BOOST_CHECK_EQUAL(fs::resolve("./",  ".foo"), ".foo");
    BOOST_CHECK_EQUAL(fs::resolve("..",  ".foo"), "../.foo");
    BOOST_CHECK_EQUAL(fs::resolve("../", ".foo"), "../.foo");

    BOOST_CHECK_EQUAL(fs::resolve("/",    ".foo"), "/.foo");
    BOOST_CHECK_EQUAL(fs::resolve("/.",   ".foo"), "/.foo");
    BOOST_CHECK_EQUAL(fs::resolve("/./",  ".foo"), "/.foo");
    BOOST_CHECK_EQUAL(fs::resolve("/..",  ".foo"), "/.foo");
    BOOST_CHECK_EQUAL(fs::resolve("/../", ".foo"), "/.foo");

    BOOST_CHECK_EQUAL(fs::resolve("a", "b"), "a/b");
    BOOST_CHECK_EQUAL(fs::resolve("a/", "b"), "a/b");
    BOOST_CHECK_EQUAL(fs::resolve("a", "b/"), "a/b/");
    BOOST_CHECK_EQUAL(fs::resolve("a", "/b"), "/b");
    BOOST_CHECK_EQUAL(fs::resolve("a/b", "c"), "a/b/c");
    BOOST_CHECK_EQUAL(fs::resolve("a/b", "../c"), "a/c");
    BOOST_CHECK_EQUAL(fs::resolve("a/b", "../../c"), "c");
    BOOST_CHECK_EQUAL(fs::resolve("a/b", "../../../c"), "../c");
    BOOST_CHECK_EQUAL(fs::resolve("/a/b", "c"), "/a/b/c");
    BOOST_CHECK_EQUAL(fs::resolve("/a/b", "../c"), "/a/c");
    BOOST_CHECK_EQUAL(fs::resolve("/a/b", "../../c"), "/c");
    BOOST_CHECK_EQUAL(fs::resolve("/a/b", "../../../c"), "/c");

    BOOST_CHECK_EQUAL(fs::resolve("s:",  ""  ), "s:");
    BOOST_CHECK_EQUAL(fs::resolve("s:",  "p" ), "s:p");
    BOOST_CHECK_EQUAL(fs::resolve("s:",  "/p"), "s:/p");
    BOOST_CHECK_EQUAL(fs::resolve("//a", ""  ), "//a");
    BOOST_CHECK_EQUAL(fs::resolve("//a", "p" ), "//a/p");
    BOOST_CHECK_EQUAL(fs::resolve("//a", "/p"), "//a/p");
    
    // Test URI path resolution
    BOOST_CHECK_EQUAL(fs::resolve("x:a", "b"), "x:a/b");
    BOOST_CHECK_EQUAL(fs::resolve("x:a", "y:b"), "y:b");
    BOOST_CHECK_EQUAL(fs::resolve("x:a", "/b"), "x:/b");
    BOOST_CHECK_EQUAL(fs::resolve("x:a?q", "b"), "x:a/b");
    BOOST_CHECK_EQUAL(fs::resolve("x:a?q", "b?r"), "x:a/b?r");
    BOOST_CHECK_EQUAL(fs::resolve("x:a?q#f", "b?r"), "x:a/b?r");
    BOOST_CHECK_EQUAL(fs::resolve("x:a?q#f", "b?r#g"), "x:a/b?r#g");
}

BOOST_AUTO_TEST_CASE(test_basename)
{
    // Test basename
    BOOST_CHECK_EQUAL(fs::basename(""), "");
    BOOST_CHECK_EQUAL(fs::basename("."), ".");
    BOOST_CHECK_EQUAL(fs::basename(".."), "..");
    BOOST_CHECK_EQUAL(fs::basename("/"), "");
    BOOST_CHECK_EQUAL(fs::basename("/a"), "a");
    BOOST_CHECK_EQUAL(fs::basename("//a"), "");
    BOOST_CHECK_EQUAL(fs::basename("foo"), "foo");
    BOOST_CHECK_EQUAL(fs::basename("foo/"), "");
    BOOST_CHECK_EQUAL(fs::basename("foo.ext"), "foo.ext");
    BOOST_CHECK_EQUAL(fs::basename("/foo.ext"), "foo.ext");
    BOOST_CHECK_EQUAL(fs::basename("bar/foo.ext"), "foo.ext");
    BOOST_CHECK_EQUAL(fs::basename("bar/foo.ext/"), "");
    BOOST_CHECK_EQUAL(fs::basename("/bar/foo.ext"), "foo.ext");
    BOOST_CHECK_EQUAL(fs::basename("scheme:/bar/foo.ext"), "foo.ext");
    BOOST_CHECK_EQUAL(fs::basename("scheme://auth/bar/foo.ext"), "foo.ext");
    BOOST_CHECK_EQUAL(fs::basename("scheme://auth/bar/foo.ext?query"), "foo.ext");
    BOOST_CHECK_EQUAL(fs::basename("scheme://auth/bar/foo.ext#frag"), "foo.ext");
    BOOST_CHECK_EQUAL(fs::basename("scheme://auth/bar/foo.ext?query#frag"), "foo.ext");
    BOOST_CHECK_EQUAL(fs::basename("//auth/bar/foo.ext?query#frag"), "foo.ext");
    BOOST_CHECK_EQUAL(fs::basename("///bar/foo.ext?query#frag"), "foo.ext");
}

BOOST_AUTO_TEST_CASE(test_dirname)
{
    // Test dirname
    BOOST_CHECK_EQUAL(fs::dirname(""), "");
    BOOST_CHECK_EQUAL(fs::dirname("/"), "");
    BOOST_CHECK_EQUAL(fs::dirname("a"), "");
    BOOST_CHECK_EQUAL(fs::dirname("/a"), "");
    BOOST_CHECK_EQUAL(fs::dirname("a/"), "a");
    BOOST_CHECK_EQUAL(fs::dirname("/a/"), "/a");
    BOOST_CHECK_EQUAL(fs::dirname("a/b"), "a");
    BOOST_CHECK_EQUAL(fs::dirname("a/b/"), "a/b");
    BOOST_CHECK_EQUAL(fs::dirname("/a/b"), "/a");
    BOOST_CHECK_EQUAL(fs::dirname("/a/b/"), "/a/b");
    BOOST_CHECK_EQUAL(fs::dirname("//a"), "");
    BOOST_CHECK_EQUAL(fs::dirname("//a/"), "");
    BOOST_CHECK_EQUAL(fs::dirname("//a/b"), "");
    BOOST_CHECK_EQUAL(fs::dirname("//a/b/"), "/b");
    BOOST_CHECK_EQUAL(fs::dirname("scheme:///bar/foo.ext?query#frag"), "/bar");
    BOOST_CHECK_EQUAL(fs::dirname("scheme:///bar/foo.ext/?query#frag"), "/bar/foo.ext");
}

BOOST_AUTO_TEST_CASE(test_extension)
{
    // Test extension
    BOOST_CHECK_EQUAL(fs::extension("scheme:///bar/foo.ext?query#frag"), ".ext");
    BOOST_CHECK_EQUAL(fs::extension("scheme:///bar/.ext?query#frag"), "");
    BOOST_CHECK_EQUAL(fs::extension("scheme:///bar/foo.ext/?query#frag"), "");
    BOOST_CHECK_EQUAL(fs::extension("scheme:///bar/foo.ext.gah?query#frag"), ".gah");
    BOOST_CHECK_EQUAL(fs::extension("scheme:///bar/foo?query#frag"), "");
}

BOOST_AUTO_TEST_CASE(test_basename_strip)
{
    // Test basename with extension stripping
    BOOST_CHECK_EQUAL(fs::basename("s://h/x.a/?q#f", true), "");
    BOOST_CHECK_EQUAL(fs::basename("s://h/x.a?q#f", true), "x");
    BOOST_CHECK_EQUAL(fs::basename("s://h/.a?q#f", true), ".a");
}

BOOST_AUTO_TEST_CASE(test_changeExtension)
{
    // Test changeExtension
    BOOST_CHECK_EQUAL(fs::changeExtension("s://h/x.a/?q#f", ""), "s://h/x.a/?q#f");
    BOOST_CHECK_EQUAL(fs::changeExtension("s://h/.a?q#f", ""), "s://h/.a?q#f");
    BOOST_CHECK_EQUAL(fs::changeExtension("s://h/x.a/?q#f", ".b"), "s://h/x.a/?q#f");
    BOOST_CHECK_EQUAL(fs::changeExtension("s://h/x.a/y?q#f", ".b"), "s://h/x.a/y.b?q#f");
    BOOST_CHECK_EQUAL(fs::changeExtension("s://h/x.a?q#f", ""), "s://h/x?q#f");
    BOOST_CHECK_EQUAL(fs::changeExtension("s://h/x.a?q#f", ".b"), "s://h/x.b?q#f");
}
