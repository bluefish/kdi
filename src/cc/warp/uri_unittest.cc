//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-05-01
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
#include <warp/uri.h>
#include <warp/strref.h>
#include <warp/vstring.h>
#include <ex/exception.h>
#include <boost/test/test_tools.hpp>

using namespace warp;
using namespace ex;

namespace
{
    VString r(char const * base, char const * ref, int allowed)
    {
        Uri baseUri(base);
        Uri refUri(ref);
        VString out;
        baseUri.resolve(refUri, out, allowed);
        return out;
    }

    /// Parse a URI and verify that its components are equal to the
    /// given values.  Since Uri makes a distinction for undefined
    /// vs. empty components, the given values may be NULL, meaning
    /// that they are undefined (and also empty).  Uri.path is always
    /// defined, so the path component cannot be null.
    boost::test_tools::predicate_result
    uriEqual(char const * uri, char const * scheme, char const * authority,
             char const * path, char const * query, char const * fragment)
    {
        if(!uri)
            raise<ValueError>("null uri");
        if(!path)
            raise<ValueError>("null path");

        bool schemeDefined     = (scheme != 0);
        bool authorityDefined  = (authority != 0);
        bool queryDefined      = (query != 0);
        bool fragmentDefined   = (fragment != 0);
        
        if(!schemeDefined)       scheme = "";
        if(!authorityDefined)    authority = "";
        if(!queryDefined)        query = "";
        if(!fragmentDefined)     fragment = "";

        boost::test_tools::predicate_result r = true;

        Uri u(uri);

        // Make sure components are what we expect
        if(u.scheme != string_wrapper(scheme))
        {
            r = false;
            r.message() << "\n  u.scheme != scheme ("
                        << u.scheme << " != " << scheme << ")";
        }
        if(u.authority != string_wrapper(authority))
        {
            r = false;
            r.message() << "\n  u.authority != authority ("
                        << u.authority << " != " << authority << ")";
        }
        if(u.path != string_wrapper(path))
        {
            r = false;
            r.message() << "\n  u.path != path ("
                        << u.path << " != " << path << ")";
        }
        if(u.query != string_wrapper(query))
        {
            r = false;
            r.message() << "\n  u.query != query ("
                        << u.query << " != " << query << ")";
        }
        if(u.fragment != string_wrapper(fragment))
        {
            r = false;
            r.message() << "\n  u.fragment != fragment ("
                        << u.fragment << " != " << fragment << ")";
        }

        // Make sure defined parts are what we expect
        if(u.schemeDefined() != schemeDefined)
        {
            r = false;
            r.message() << "\n  u.schemeDefined() != schemeDefined ("
                        << u.schemeDefined() << " != "
                        << schemeDefined << ")";
        }
        if(u.authorityDefined() != authorityDefined)
        {
            r = false;
            r.message() << "\n  u.authorityDefined() != authorityDefined ("
                        << u.authorityDefined() << " != "
                        << authorityDefined << ")";
        }
        if(u.queryDefined() != queryDefined)
        {
            r = false;
            r.message() << "\n  u.queryDefined() != queryDefined ("
                        << u.queryDefined() << " != "
                        << queryDefined << ")";
        }
        if(u.fragmentDefined() != fragmentDefined)
        {
            r = false;
            r.message() << "\n  u.fragmentDefined() != fragmentDefined ("
                        << u.fragmentDefined() << " != "
                        << fragmentDefined << ")";
        }

        return r;
    }
}

BOOST_AUTO_TEST_CASE(uri_parse)
{
    // Check basic parsing
    BOOST_CHECK(uriEqual("s://a/b/c/d;p?q#f", "s", "a", "/b/c/d;p", "q", "f"));
    
    // One missing component
    BOOST_CHECK(uriEqual("//a/b/c/d;p?q#f", 0, "a", "/b/c/d;p", "q", "f"));
    BOOST_CHECK(uriEqual("s:/b/c/d;p?q#f", "s", 0, "/b/c/d;p", "q", "f"));
    BOOST_CHECK(uriEqual("s://a?q#f", "s", "a", "", "q", "f"));
    BOOST_CHECK(uriEqual("s://a/b/c/d;p#f", "s", "a", "/b/c/d;p", 0, "f"));
    BOOST_CHECK(uriEqual("s://a/b/c/d;p?q", "s", "a", "/b/c/d;p", "q", 0));

    // Two missing components
    BOOST_CHECK(uriEqual("/b/c/d;p?q#f", 0, 0, "/b/c/d;p", "q", "f"));
    BOOST_CHECK(uriEqual("//a?q#f", 0, "a", "", "q", "f"));
    BOOST_CHECK(uriEqual("//a/b/c/d;p#f", 0, "a", "/b/c/d;p", 0, "f"));
    BOOST_CHECK(uriEqual("//a/b/c/d;p?q", 0, "a", "/b/c/d;p", "q", 0));

    BOOST_CHECK(uriEqual("s:?q#f", "s", 0, "", "q", "f"));
    BOOST_CHECK(uriEqual("s:/b/c/d;p#f", "s", 0, "/b/c/d;p", 0, "f"));
    BOOST_CHECK(uriEqual("s:/b/c/d;p?q", "s", 0, "/b/c/d;p", "q", 0));

    BOOST_CHECK(uriEqual("s://a#f", "s", "a", "", 0, "f"));
    BOOST_CHECK(uriEqual("s://a?q", "s", "a", "", "q", 0));

    BOOST_CHECK(uriEqual("s://a/b/c/d;p", "s", "a", "/b/c/d;p", 0, 0));

    // Three missing components
    BOOST_CHECK(uriEqual("?q#f", 0, 0, "", "q", "f"));
    BOOST_CHECK(uriEqual("/b/c/d;p#f", 0, 0, "/b/c/d;p", 0, "f"));
    BOOST_CHECK(uriEqual("/b/c/d;p?q", 0, 0, "/b/c/d;p", "q", 0));

    BOOST_CHECK(uriEqual("//a#f", 0, "a", "", 0, "f"));
    BOOST_CHECK(uriEqual("//a?q", 0, "a", "", "q", 0));

    BOOST_CHECK(uriEqual("//a/b/c/d;p", 0, "a", "/b/c/d;p", 0, 0));

    BOOST_CHECK(uriEqual("s:#f", "s", 0, "", 0, "f"));
    BOOST_CHECK(uriEqual("s:?q", "s", 0, "", "q", 0));

    BOOST_CHECK(uriEqual("s:/b/c/d;p", "s", 0, "/b/c/d;p", 0, 0));

    BOOST_CHECK(uriEqual("s://a", "s", "a", "", 0, 0));

    // Four missing components
    BOOST_CHECK(uriEqual("s:", "s", 0, "", 0, 0));
    BOOST_CHECK(uriEqual("//a", 0, "a", "", 0, 0));
    BOOST_CHECK(uriEqual("/b/c/d;p", 0, 0, "/b/c/d;p", 0, 0));
    BOOST_CHECK(uriEqual("?q", 0, 0, "", "q", 0));
    BOOST_CHECK(uriEqual("#f", 0, 0, "", 0, "f"));

    // Five missing components
    BOOST_CHECK(uriEqual("", 0, 0, "", 0, 0));
}

BOOST_AUTO_TEST_CASE(uri_parse_tricky)
{
    // Valid schemes are: [A-Za-z][A-Za-z0-9.+-]*
    BOOST_CHECK(uriEqual(":", 0, 0, ":", 0, 0));
    BOOST_CHECK(uriEqual("a:", "a", 0, "", 0, 0));
    BOOST_CHECK(uriEqual("1:", 0, 0, "1:", 0, 0));
    BOOST_CHECK(uriEqual("a/2:c", 0, 0, "a/2:c", 0, 0));
    BOOST_CHECK(uriEqual("a.2:c", "a.2", 0, "c", 0, 0));
    BOOST_CHECK(uriEqual("a+2:c", "a+2", 0, "c", 0, 0));
    BOOST_CHECK(uriEqual("a-2:c", "a-2", 0, "c", 0, 0));
    BOOST_CHECK(uriEqual("a_2:c", 0, 0, "a_2:c", 0, 0));
}

BOOST_AUTO_TEST_CASE(resolve_rfc_3986)
{
    // The following tests come from RFC 3986.


    // 5.4.  Reference Resolution Examples
    // 
    //    Within a representation with a well defined base URI of
    // 
    //       http://a/b/c/d;p?q
    // 
    //    a relative reference is transformed to its target URI as follows.
    
    // 5.4.1.  Normal Examples

    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g:h",     Uri::RFC_3986), "g:h");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g",       Uri::RFC_3986), "http://a/b/c/g");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "./g",     Uri::RFC_3986), "http://a/b/c/g");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g/",      Uri::RFC_3986), "http://a/b/c/g/");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "/g",      Uri::RFC_3986), "http://a/g");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "//g",     Uri::RFC_3986), "http://g");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "?y",      Uri::RFC_3986), "http://a/b/c/d;p?y");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g?y",     Uri::RFC_3986), "http://a/b/c/g?y");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "#s",      Uri::RFC_3986), "http://a/b/c/d;p?q#s");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g#s",     Uri::RFC_3986), "http://a/b/c/g#s");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g?y#s",   Uri::RFC_3986), "http://a/b/c/g?y#s");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", ";x",      Uri::RFC_3986), "http://a/b/c/;x");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g;x",     Uri::RFC_3986), "http://a/b/c/g;x");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g;x?y#s", Uri::RFC_3986), "http://a/b/c/g;x?y#s");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "",        Uri::RFC_3986), "http://a/b/c/d;p?q");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", ".",       Uri::RFC_3986), "http://a/b/c/");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "./",      Uri::RFC_3986), "http://a/b/c/");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "..",      Uri::RFC_3986), "http://a/b/");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "../",     Uri::RFC_3986), "http://a/b/");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "../g",    Uri::RFC_3986), "http://a/b/g");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "../..",   Uri::RFC_3986), "http://a/");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "../../",  Uri::RFC_3986), "http://a/");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "../../g", Uri::RFC_3986), "http://a/g");

    // 5.4.2.  Abnormal Examples
    // 
    //    Although the following abnormal examples are unlikely to occur in
    //    normal practice, all URI parsers should be capable of resolving them
    //    consistently.  Each example uses the same base as that above.
    // 
    //    Parsers must be careful in handling cases where there are more ".."
    //    segments in a relative-path reference than there are hierarchical
    //    levels in the base URI's path.  Note that the ".." syntax cannot be
    //    used to change the authority component of a URI.

    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "../../../g",    Uri::RFC_3986), "http://a/g");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "../../../../g", Uri::RFC_3986), "http://a/g");


    //    Similarly, parsers must remove the dot-segments "." and ".." when
    //    they are complete components of a path, but not when they are only
    //    part of a segment.

    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "/./g",  Uri::RFC_3986), "http://a/g");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "/../g", Uri::RFC_3986), "http://a/g");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g.",    Uri::RFC_3986), "http://a/b/c/g.");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", ".g",    Uri::RFC_3986), "http://a/b/c/.g");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g..",   Uri::RFC_3986), "http://a/b/c/g..");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "..g",   Uri::RFC_3986), "http://a/b/c/..g");

    //    Less likely are cases where the relative reference uses unnecessary
    //    or nonsensical forms of the "." and ".." complete path segments.

    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "./../g",     Uri::RFC_3986), "http://a/b/g");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "./g/.",      Uri::RFC_3986), "http://a/b/c/g/");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g/./h",      Uri::RFC_3986), "http://a/b/c/g/h");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g/../h",     Uri::RFC_3986), "http://a/b/c/h");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g;x=1/./y",  Uri::RFC_3986), "http://a/b/c/g;x=1/y");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g;x=1/../y", Uri::RFC_3986), "http://a/b/c/y");

    //    Some applications fail to separate the reference's query and/or
    //    fragment components from the path component before merging it with
    //    the base path and removing dot-segments.  This error is rarely
    //    noticed, as typical usage of a fragment never includes the hierarchy
    //    ("/") character and the query component is not normally used within
    //    relative references.

    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g?y/./x",  Uri::RFC_3986), "http://a/b/c/g?y/./x");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g?y/../x", Uri::RFC_3986), "http://a/b/c/g?y/../x");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g#s/./x",  Uri::RFC_3986), "http://a/b/c/g#s/./x");
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "g#s/../x", Uri::RFC_3986), "http://a/b/c/g#s/../x");

    //    Some parsers allow the scheme name to be present in a relative
    //    reference if it is the same as the base URI scheme.  This is
    //    considered to be a loophole in prior specifications of partial URI
    //    [RFC1630].  Its use should be avoided but is allowed for backward
    //    compatibility.

    // for strict parsers
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "http:g", Uri::RFC_3986), "http:g");
      
    // for backward compatibility
    BOOST_CHECK_EQUAL(r("http://a/b/c/d;p?q", "http:g", Uri::RFC_1630), "http://a/b/c/g");
}

BOOST_AUTO_TEST_CASE(resolve_relative)
{
    // The URI RFC only defines how resolution happens for absolute
    // base URIs.  We would like to allow relative resolution in a
    // useful way as well.  This is especially useful for handling
    // filesystem paths as URIs.


    // These tests are taken from the RFC examples, and modified to
    // use a relative path.

    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g:h",     Uri::STRICT), "g:h");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g",       Uri::STRICT), "b/c/g");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "./g",     Uri::STRICT), "b/c/g");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g/",      Uri::STRICT), "b/c/g/");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "/g",      Uri::STRICT), "/g");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "//g",     Uri::STRICT), "//g");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "?y",      Uri::STRICT), "b/c/d;p?y");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g?y",     Uri::STRICT), "b/c/g?y");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "#s",      Uri::STRICT), "b/c/d;p?q#s");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g#s",     Uri::STRICT), "b/c/g#s");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g?y#s",   Uri::STRICT), "b/c/g?y#s");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", ";x",      Uri::STRICT), "b/c/;x");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g;x",     Uri::STRICT), "b/c/g;x");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g;x?y#s", Uri::STRICT), "b/c/g;x?y#s");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "",        Uri::STRICT), "b/c/d;p?q");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", ".",       Uri::STRICT), "b/c/");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "./",      Uri::STRICT), "b/c/");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "..",      Uri::STRICT), "b/");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "../",     Uri::STRICT), "b/");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "../g",    Uri::STRICT), "b/g");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "../..",   Uri::STRICT), "");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "../../",  Uri::STRICT), "");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "../../g", Uri::STRICT), "g");



    //    Parsers must be careful in handling cases where there are more ".."
    //    segments in a relative-path reference than there are hierarchical
    //    levels in the base URI's path.  Note that the ".." syntax cannot be
    //    used to change the authority component of a URI.

    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "../../../g",    Uri::RFC_3986), "../g");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "../../../../g", Uri::RFC_3986), "../../g");


    //    Similarly, parsers must remove the dot-segments "." and ".." when
    //    they are complete components of a path, but not when they are only
    //    part of a segment.

    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "/./g",  Uri::RFC_3986), "/g");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "/../g", Uri::RFC_3986), "/g");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g.",    Uri::RFC_3986), "b/c/g.");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", ".g",    Uri::RFC_3986), "b/c/.g");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g..",   Uri::RFC_3986), "b/c/g..");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "..g",   Uri::RFC_3986), "b/c/..g");

    //    Less likely are cases where the relative reference uses unnecessary
    //    or nonsensical forms of the "." and ".." complete path segments.

    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "./../g",     Uri::RFC_3986), "b/g");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "./g/.",      Uri::RFC_3986), "b/c/g/");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g/./h",      Uri::RFC_3986), "b/c/g/h");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g/../h",     Uri::RFC_3986), "b/c/h");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g;x=1/./y",  Uri::RFC_3986), "b/c/g;x=1/y");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g;x=1/../y", Uri::RFC_3986), "b/c/y");

    //    Some applications fail to separate the reference's query and/or
    //    fragment components from the path component before merging it with
    //    the base path and removing dot-segments.  This error is rarely
    //    noticed, as typical usage of a fragment never includes the hierarchy
    //    ("/") character and the query component is not normally used within
    //    relative references.

    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g?y/./x",  Uri::RFC_3986), "b/c/g?y/./x");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g?y/../x", Uri::RFC_3986), "b/c/g?y/../x");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g#s/./x",  Uri::RFC_3986), "b/c/g#s/./x");
    BOOST_CHECK_EQUAL(r("b/c/d;p?q", "g#s/../x", Uri::RFC_3986), "b/c/g#s/../x");
}

BOOST_AUTO_UNIT_TEST(uri_get_param)
{
    BOOST_CHECK_EQUAL(uriGetParameter("", "x"), "");
    BOOST_CHECK_EQUAL(uriGetParameter("?x", "x"), "");
    BOOST_CHECK_EQUAL(uriGetParameter("?x=", "x"), "");
    BOOST_CHECK_EQUAL(uriGetParameter("?x=y", "x"), "y");
    BOOST_CHECK_EQUAL(uriGetParameter("?x=z&x=y", "x"), "z");

    BOOST_CHECK_EQUAL(uriGetParameter("http://foo.com/duh.cgi", "x"), "");
    BOOST_CHECK_EQUAL(uriGetParameter("http://foo.com/duh.cgi?x", "x"), "");
    BOOST_CHECK_EQUAL(uriGetParameter("http://foo.com/duh.cgi?x=", "x"), "");
    BOOST_CHECK_EQUAL(uriGetParameter("http://foo.com/duh.cgi?x=y", "x"), "y");
    BOOST_CHECK_EQUAL(uriGetParameter("http://foo.com/duh.cgi?x=z&x=y", "x"), "z");
    BOOST_CHECK_EQUAL(uriGetParameter("http://foo.com/duh.cgi?x=z&x=y#f", "x"), "z");
}

BOOST_AUTO_UNIT_TEST(uri_set_param)
{
    BOOST_CHECK_EQUAL(uriSetParameter("", "x", "v"), "?x=v");
    BOOST_CHECK_EQUAL(uriSetParameter("?y=x", "x", "v"), "?y=x&x=v");
    BOOST_CHECK_EQUAL(uriSetParameter("?x", "x", "v"), "?x=v");
    BOOST_CHECK_EQUAL(uriSetParameter("?x=", "x", "v"), "?x=v");
    BOOST_CHECK_EQUAL(uriSetParameter("?x=y", "x", "v"), "?x=v");
    BOOST_CHECK_EQUAL(uriSetParameter("?x=z&x=y", "x", "v"), "?x=v");
    BOOST_CHECK_EQUAL(uriSetParameter("?x=z&y=x&x=y", "x", "v"), "?x=v&y=x");

    BOOST_CHECK_EQUAL(uriSetParameter("", "x", ""), "?x");
    BOOST_CHECK_EQUAL(uriSetParameter("?y=x", "x", ""), "?y=x&x");
    BOOST_CHECK_EQUAL(uriSetParameter("?x", "x", ""), "?x");
    BOOST_CHECK_EQUAL(uriSetParameter("?x=", "x", ""), "?x");
    BOOST_CHECK_EQUAL(uriSetParameter("?x=y", "x", ""), "?x");
    BOOST_CHECK_EQUAL(uriSetParameter("?x=z&x=y", "x", ""), "?x");
    BOOST_CHECK_EQUAL(uriSetParameter("?x=z&y=x&x=y", "x", ""), "?x&y=x");

    BOOST_CHECK_EQUAL(uriSetParameter("http://foo.com/duh.cgi", "x", "v"), "http://foo.com/duh.cgi?x=v");
    BOOST_CHECK_EQUAL(uriSetParameter("http://foo.com/duh.cgi?y=x", "x", "v"), "http://foo.com/duh.cgi?y=x&x=v");
    BOOST_CHECK_EQUAL(uriSetParameter("http://foo.com/duh.cgi?x", "x", "v"), "http://foo.com/duh.cgi?x=v");
    BOOST_CHECK_EQUAL(uriSetParameter("http://foo.com/duh.cgi?x=", "x", "v"), "http://foo.com/duh.cgi?x=v");
    BOOST_CHECK_EQUAL(uriSetParameter("http://foo.com/duh.cgi?x=y", "x", "v"), "http://foo.com/duh.cgi?x=v");
    BOOST_CHECK_EQUAL(uriSetParameter("http://foo.com/duh.cgi?x=z&x=y", "x", "v"), "http://foo.com/duh.cgi?x=v");
    BOOST_CHECK_EQUAL(uriSetParameter("http://foo.com/duh.cgi?x=z&y=x&x=y", "x", "v"), "http://foo.com/duh.cgi?x=v&y=x");
    BOOST_CHECK_EQUAL(uriSetParameter("http://foo.com/duh.cgi?x=z&y=x&x=y#f", "x", "v"), "http://foo.com/duh.cgi?x=v&y=x#f");

    BOOST_CHECK_EQUAL(uriSetParameter("http://foo.com/duh.cgi", "x", ""), "http://foo.com/duh.cgi?x");
    BOOST_CHECK_EQUAL(uriSetParameter("http://foo.com/duh.cgi?y=x", "x", ""), "http://foo.com/duh.cgi?y=x&x");
    BOOST_CHECK_EQUAL(uriSetParameter("http://foo.com/duh.cgi?x", "x", ""), "http://foo.com/duh.cgi?x");
    BOOST_CHECK_EQUAL(uriSetParameter("http://foo.com/duh.cgi?x=", "x", ""), "http://foo.com/duh.cgi?x");
    BOOST_CHECK_EQUAL(uriSetParameter("http://foo.com/duh.cgi?x=y", "x", ""), "http://foo.com/duh.cgi?x");
    BOOST_CHECK_EQUAL(uriSetParameter("http://foo.com/duh.cgi?x=z&x=y", "x", ""), "http://foo.com/duh.cgi?x");
    BOOST_CHECK_EQUAL(uriSetParameter("http://foo.com/duh.cgi?x=z&y=x&x=y", "x", ""), "http://foo.com/duh.cgi?x&y=x");
    BOOST_CHECK_EQUAL(uriSetParameter("http://foo.com/duh.cgi?x=z&y=x&x=y#f", "x", ""), "http://foo.com/duh.cgi?x&y=x#f");
}

BOOST_AUTO_UNIT_TEST(uri_erase_param)
{
    BOOST_CHECK_EQUAL(uriEraseParameter("", "x"), "");
    BOOST_CHECK_EQUAL(uriEraseParameter("?", "x"), "");
    BOOST_CHECK_EQUAL(uriEraseParameter("?x", "x"), "");
    BOOST_CHECK_EQUAL(uriEraseParameter("?x=y", "x"), "");
    BOOST_CHECK_EQUAL(uriEraseParameter("?x=y&x=z", "x"), "");
    BOOST_CHECK_EQUAL(uriEraseParameter("?x=y&y=x", "x"), "?y=x");
    BOOST_CHECK_EQUAL(uriEraseParameter("?x=y&y=x&x=z", "x"), "?y=x");

    BOOST_CHECK_EQUAL(uriEraseParameter("http://foo.com/duh.cgi", "x"), "http://foo.com/duh.cgi");
    BOOST_CHECK_EQUAL(uriEraseParameter("http://foo.com/duh.cgi?", "x"), "http://foo.com/duh.cgi");
    BOOST_CHECK_EQUAL(uriEraseParameter("http://foo.com/duh.cgi?x", "x"), "http://foo.com/duh.cgi");
    BOOST_CHECK_EQUAL(uriEraseParameter("http://foo.com/duh.cgi?x=y", "x"), "http://foo.com/duh.cgi");
    BOOST_CHECK_EQUAL(uriEraseParameter("http://foo.com/duh.cgi?x=y&x=z", "x"), "http://foo.com/duh.cgi");
    BOOST_CHECK_EQUAL(uriEraseParameter("http://foo.com/duh.cgi?x=y&y=x", "x"), "http://foo.com/duh.cgi?y=x");
    BOOST_CHECK_EQUAL(uriEraseParameter("http://foo.com/duh.cgi?x=y&y=x&x=z", "x"), "http://foo.com/duh.cgi?y=x");
    BOOST_CHECK_EQUAL(uriEraseParameter("http://foo.com/duh.cgi?x=y&y=x&x=z#f", "x"), "http://foo.com/duh.cgi?y=x#f");
}

BOOST_AUTO_UNIT_TEST(uri_resolve)
{
    // Make sure convenience forwarding function works as expected.
    // Leave the tricky conformance testing to other functions.
    BOOST_CHECK_EQUAL(uriResolve("http://foo.com", "index.html"), "http://foo.com/index.html");
    BOOST_CHECK_EQUAL(uriResolve("http://foo.com/index.html", "logo.png"), "http://foo.com/logo.png");
}


BOOST_AUTO_UNIT_TEST(uri_abtl_case)
{
    std::string base(
        "http://www.myride.com/research/editorial/makeandmodel/mixedsources"
        ".html;jsessionid=8pw1GMTc2dJqnJ1ZWjCvbspK106TGx1v28bB0X2Lr2v4T2JPt"
        "P5n!1396309876?editorialId=3318");

    std::string ref("?editorialId=3318&pageNum=2");

    std::string firefoxResolved(
        "http://www.myride.com/research/editorial/makeandmodel/mixedsources"
        ".html;jsessionid=8pw1GMTc2dJqnJ1ZWjCvbspK106TGx1v28bB0X2Lr2v4T2JPt"
        "P5n!1396309876?editorialId=3318&pageNum=2");

    std::string pythonResolved(
        "http://www.myride.com/research/editorial/makeandmodel/mixedsources"
        ".html;jsessionid=8pw1GMTc2dJqnJ1ZWjCvbspK106TGx1v28bB0X2Lr2v4T2JPt"
        "P5n!1396309876?editorialId=3318&pageNum=2/?editorialId=3318&pageNu"
        "m=2");

    BOOST_CHECK_EQUAL(uriResolve(base, ref), firefoxResolved);
}

BOOST_AUTO_UNIT_TEST(uri_popscheme)
{
    BOOST_CHECK_EQUAL(uriPopScheme(""), "");
    BOOST_CHECK_EQUAL(uriPopScheme(":"), ":");
    BOOST_CHECK_EQUAL(uriPopScheme("foo"), "foo");
    BOOST_CHECK_EQUAL(uriPopScheme("foo:"), "");
    BOOST_CHECK_EQUAL(uriPopScheme("foo:bar"), "bar");
    BOOST_CHECK_EQUAL(uriPopScheme("foo+bar:"), "bar:");
    BOOST_CHECK_EQUAL(uriPopScheme("foo+bar:baz"), "bar:baz");
}

BOOST_AUTO_UNIT_TEST(uri_topscheme)
{
    BOOST_CHECK_EQUAL(uriTopScheme(""), "");
    BOOST_CHECK_EQUAL(uriTopScheme(":"), "");
    BOOST_CHECK_EQUAL(uriTopScheme("foo"), "");
    BOOST_CHECK_EQUAL(uriTopScheme("foo:"), "foo");
    BOOST_CHECK_EQUAL(uriTopScheme("foo:bar"), "foo");
    BOOST_CHECK_EQUAL(uriTopScheme("foo+bar:"), "foo");
    BOOST_CHECK_EQUAL(uriTopScheme("foo+bar:baz"), "foo");
}

BOOST_AUTO_UNIT_TEST(uri_decode)
{
    BOOST_CHECK_EQUAL(uriDecode(""), "");
    BOOST_CHECK_EQUAL(uriDecode("%2"), "");
    BOOST_CHECK_EQUAL(uriDecode("%20"), " ");
    BOOST_CHECK_EQUAL(uriDecode("w%xyz"), "wz");
    BOOST_CHECK_EQUAL(uriDecode("foo%20bar"), "foo bar");
    BOOST_CHECK_EQUAL(uriDecode("foo+bar"), "foo+bar");
    BOOST_CHECK_EQUAL(uriDecode("%FF%01%e7foo+%20bar%99+%55"), "\xff\x01\xe7" "foo+ bar\x99+\x55");

    BOOST_CHECK_EQUAL(uriDecode("", true), "");
    BOOST_CHECK_EQUAL(uriDecode("%2", true), "");
    BOOST_CHECK_EQUAL(uriDecode("%20", true), " ");
    BOOST_CHECK_EQUAL(uriDecode("w%xyz", true), "wz");
    BOOST_CHECK_EQUAL(uriDecode("foo%20bar", true), "foo bar");
    BOOST_CHECK_EQUAL(uriDecode("foo+bar", true), "foo bar");
    BOOST_CHECK_EQUAL(uriDecode("%FF%01%e7foo+%20bar%99+%55", true), "\xff\x01\xe7" "foo  bar\x99 \x55");
}

BOOST_AUTO_UNIT_TEST(uri_encode)
{
    // Default encoding -- somewhat permissive on safe characters and
    // allows space-to-plus conversion
    BOOST_CHECK_EQUAL(uriEncode("", true), "");
    BOOST_CHECK_EQUAL(uriEncode("`~!@#$%^&*()_-+=", true), "%60~!%40%23%24%25%5e%26*()_-%2b%3d");
    BOOST_CHECK_EQUAL(uriEncode("{}[]\\|:;\"'<>,./?", true), "%7b%7d%5b%5d%5c%7c:%3b%22'%3c%3e,./%3f");
    BOOST_CHECK_EQUAL(uriEncode("abcdEFGH6789", true), "abcdEFGH6789");
    BOOST_CHECK_EQUAL(uriEncode("hi there", true), "hi+there");

    // Strict encoding -- no reserved characters, only percent
    // encoding
    BOOST_CHECK_EQUAL(uriEncode("", false, ""), "");
    BOOST_CHECK_EQUAL(uriEncode("`~!@#$%^&*()_-+=", false, ""), "%60~%21%40%23%24%25%5e%26%2a%28%29_-%2b%3d");
    BOOST_CHECK_EQUAL(uriEncode("{}[]\\|:;\"'<>,./?", false, ""), "%7b%7d%5b%5d%5c%7c%3a%3b%22%27%3c%3e%2c.%2f%3f");
    BOOST_CHECK_EQUAL(uriEncode("abcdEFGH6789", false, ""), "abcdEFGH6789");
    BOOST_CHECK_EQUAL(uriEncode("hi there", false, ""), "hi%20there");
}

BOOST_AUTO_UNIT_TEST(uri_normalize)
{
    BOOST_CHECK_EQUAL(uriNormalize(""), "");
    BOOST_CHECK_EQUAL(uriNormalize("//"), "//");
    BOOST_CHECK_EQUAL(uriNormalize(":"), ":");

    BOOST_CHECK_EQUAL(uriNormalize("some/path"), "some/path");
    BOOST_CHECK_EQUAL(uriNormalize("some//path"), "some/path");
    BOOST_CHECK_EQUAL(uriNormalize("./some//path/..///stuff/./."), "some/stuff/");
    BOOST_CHECK_EQUAL(uriNormalize("/space out/path/"), "/space%20out/path/");
    BOOST_CHECK_EQUAL(uriNormalize("not/a/scheme:"), "not/a/scheme:");
    BOOST_CHECK_EQUAL(uriNormalize("%6F%76%65%72%65%6E%63%6F%64%65%64%2F%70%61%74%68"), "overencoded/path");
    BOOST_CHECK_EQUAL(uriNormalize("path/containing/a+plus"), "path/containing/a+plus");
    
    BOOST_CHECK_EQUAL(uriNormalize("?"), "");
    BOOST_CHECK_EQUAL(uriNormalize("?q=x"), "?q=x");
    BOOST_CHECK_EQUAL(uriNormalize("?q==x"), "?q=%3dx");
    BOOST_CHECK_EQUAL(uriNormalize("?x="), "?x"); // no value, drop '='
    BOOST_CHECK_EQUAL(uriNormalize("?x=&"), "?x"); // no value, drop '=', drop trailing '&'
    BOOST_CHECK_EQUAL(uriNormalize("?x=&y"), "?x&y"); // no value, drop '='
    BOOST_CHECK_EQUAL(uriNormalize("?x=&y=v"), "?x&y=v"); // no value, drop '='
    BOOST_CHECK_EQUAL(uriNormalize("?=x"), ""); // no key, drop parameter
    BOOST_CHECK_EQUAL(uriNormalize("?=x&y"), "?y"); // no key, drop parameter
    BOOST_CHECK_EQUAL(uriNormalize("?==&&x==&&z&&&=="), "?x=%3d&z"); // clean up!
    BOOST_CHECK_EQUAL(uriNormalize("?==&&x== foo&&z %20= ?&&&=="), "?x=%3d+foo&z++=+?"); // clean up spaces

    BOOST_CHECK_EQUAL(uriNormalize("#"), "");
    BOOST_CHECK_EQUAL(uriNormalize("#f"), "#f");
    BOOST_CHECK_EQUAL(uriNormalize("#f stop"), "#f%20stop");

    BOOST_CHECK_EQUAL(uriNormalize("http://www.example.com/index.html"), "http://www.example.com/index.html");
    BOOST_CHECK_EQUAL(uriNormalize("HTTP://www.EXAM%70%6C%65.Com///./..///./index.html?=&=#"), "http://www.example.com/index.html");

    BOOST_CHECK_EQUAL(uriNormalize("http://dont.add.slash.com"), "http://dont.add.slash.com");

    BOOST_CHECK_EQUAL(uriNormalize("http://foo.com/"), "http://foo.com/");
    BOOST_CHECK_EQUAL(uriNormalize("http://foo.com:80/"), "http://foo.com/");
    BOOST_CHECK_EQUAL(uriNormalize("http://foo.com:443/"), "http://foo.com:443/");
    BOOST_CHECK_EQUAL(uriNormalize("http://foo.com:8080/"), "http://foo.com:8080/");

    BOOST_CHECK_EQUAL(uriNormalize("https://foo.com/"), "https://foo.com/");
    BOOST_CHECK_EQUAL(uriNormalize("https://foo.com:80/"), "https://foo.com:80/");
    BOOST_CHECK_EQUAL(uriNormalize("https://foo.com:443/"), "https://foo.com/");
    BOOST_CHECK_EQUAL(uriNormalize("https://foo.com:8080/"), "https://foo.com:8080/");

    BOOST_CHECK_EQUAL(uriNormalize("ftp://foo.com/"), "ftp://foo.com/");
    BOOST_CHECK_EQUAL(uriNormalize("ftp://foo.com:21/"), "ftp://foo.com/");
    BOOST_CHECK_EQUAL(uriNormalize("ftp://foo.com:8080/"), "ftp://foo.com:8080/");

    BOOST_CHECK_EQUAL(uriNormalize("gopher://foo.com/"), "gopher://foo.com/");
    BOOST_CHECK_EQUAL(uriNormalize("gopher://foo.com:70/"), "gopher://foo.com/");
    BOOST_CHECK_EQUAL(uriNormalize("gopher://foo.com:8080/"), "gopher://foo.com:8080/");

    BOOST_CHECK_EQUAL(uriNormalize("blah://foo.com/"), "blah://foo.com/");
    BOOST_CHECK_EQUAL(uriNormalize("blah://foo.com:70/"), "blah://foo.com:70/");
    BOOST_CHECK_EQUAL(uriNormalize("blah://foo.com:8080/"), "blah://foo.com:8080/");
}

BOOST_AUTO_UNIT_TEST(uri_pushscheme)
{
    // When scheme is empty, pass through
    BOOST_CHECK_EQUAL(uriPushScheme("", ""), "");
    BOOST_CHECK_EQUAL(uriPushScheme("foo", ""), "foo");
    BOOST_CHECK_EQUAL(uriPushScheme("foo:", ""), "foo:");
    BOOST_CHECK_EQUAL(uriPushScheme("foo+bar:", ""), "foo+bar:");

    // Otherwise, prepend scheme
    BOOST_CHECK_EQUAL(uriPushScheme("", "baz"), "baz:");
    BOOST_CHECK_EQUAL(uriPushScheme("foo", "baz"), "baz:foo");
    BOOST_CHECK_EQUAL(uriPushScheme("foo:", "baz"), "baz+foo:");
    BOOST_CHECK_EQUAL(uriPushScheme("foo+bar:", "baz"), "baz+foo+bar:");
}
