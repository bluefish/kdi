//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: robots_txt_unittest.cc $
//
// Created 2008/01/20
//
// Copyright 2008 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <warp/robots_txt.h>
#include <unittest/main.h>

using namespace warp;
using namespace std;

namespace
{
    char const * TEST = 
    "# Comments appear after the \"#\" symbol\n"
    "UsER-agENT:    *   # match all bots\n"
    "Disallow:   / # keep them out\n"
    "\n"
    "some garbage\n"
    "\n"
    "  # FancyBot has carte blanche!\n"
    "User-Agent: FancyBot\n"
    "Disallow:  \n"
    "\n"
    "\n"
    "# We sorta like SpecialBot and NotSoSpecialBot\n"
    "user-agent:   SpecialBot\n"
    "User-agent: NotSoSpecialBot   \n"
    "  Allow: /searchhistory/\n"
    "Disallow: /news?output=xhtml&\n"
    "ALLOW  : /news?output=xhtml  \n"
    "# Comments in the middle\n"
    "Disallow: /search\n"
    "  DisalloW :/images  \n"
    "Disallow: /?";
}

BOOST_AUTO_UNIT_TEST(itertest)
{
    RobotsTxtIterator x(TEST, strlen(TEST));
    
    pair<string,string> kv;

    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::KEY_VALUE);
    BOOST_CHECK_EQUAL(kv.first, "user-agent");
    BOOST_CHECK_EQUAL(kv.second, "*");

    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::KEY_VALUE);
    BOOST_CHECK_EQUAL(kv.first, "disallow");
    BOOST_CHECK_EQUAL(kv.second, "/");

    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::BLANK);
    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::BLANK);

    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::KEY_VALUE);
    BOOST_CHECK_EQUAL(kv.first, "user-agent");
    BOOST_CHECK_EQUAL(kv.second, "FancyBot");

    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::KEY_VALUE);
    BOOST_CHECK_EQUAL(kv.first, "disallow");
    BOOST_CHECK_EQUAL(kv.second, "");

    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::BLANK);
    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::BLANK);

    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::KEY_VALUE);
    BOOST_CHECK_EQUAL(kv.first, "user-agent");
    BOOST_CHECK_EQUAL(kv.second, "SpecialBot");

    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::KEY_VALUE);
    BOOST_CHECK_EQUAL(kv.first, "user-agent");
    BOOST_CHECK_EQUAL(kv.second, "NotSoSpecialBot");

    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::KEY_VALUE);
    BOOST_CHECK_EQUAL(kv.first, "allow");
    BOOST_CHECK_EQUAL(kv.second, "/searchhistory/");

    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::KEY_VALUE);
    BOOST_CHECK_EQUAL(kv.first, "disallow");
    BOOST_CHECK_EQUAL(kv.second, "/news?output=xhtml&");

    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::KEY_VALUE);
    BOOST_CHECK_EQUAL(kv.first, "allow");
    BOOST_CHECK_EQUAL(kv.second, "/news?output=xhtml");

    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::KEY_VALUE);
    BOOST_CHECK_EQUAL(kv.first, "disallow");
    BOOST_CHECK_EQUAL(kv.second, "/search");

    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::KEY_VALUE);
    BOOST_CHECK_EQUAL(kv.first, "disallow");
    BOOST_CHECK_EQUAL(kv.second, "/images");

    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::KEY_VALUE);
    BOOST_CHECK_EQUAL(kv.first, "disallow");
    BOOST_CHECK_EQUAL(kv.second, "/?");

    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::END);
    BOOST_CHECK_EQUAL(x.get(kv), RobotsTxtIterator::END);
}

BOOST_AUTO_UNIT_TEST(filtertest)
{
    {
        RobotsFilter f("SillyBot");
        f.loadRobotsTxt(TEST);

        BOOST_CHECK(!f.isPathAllowed("/"));
        BOOST_CHECK(!f.isPathAllowed("/something"));
        BOOST_CHECK(!f.isPathAllowed("/?anything"));
    }

    {
        RobotsFilter f("fancyBOT");
        f.loadRobotsTxt(TEST);

        BOOST_CHECK(f.isPathAllowed("/"));
        BOOST_CHECK(f.isPathAllowed("/something"));
        BOOST_CHECK(f.isPathAllowed("/?anything"));
    }

    {
        RobotsFilter f("specialbot");
        f.loadRobotsTxt(TEST);

        BOOST_CHECK( f.isPathAllowed("/"));
        BOOST_CHECK(!f.isPathAllowed("/search"));
        BOOST_CHECK(!f.isPathAllowed("/searchstuff/foo"));
        BOOST_CHECK( f.isPathAllowed("/searchhistory/foo"));
        BOOST_CHECK( f.isPathAllowed("/banjos/index.html"));
        BOOST_CHECK(!f.isPathAllowed("/?anything"));
        BOOST_CHECK( f.isPathAllowed("/news?output=xhtml"));
        BOOST_CHECK( f.isPathAllowed("/news?output=xhtml2"));
        BOOST_CHECK(!f.isPathAllowed("/news?output=xhtml&page=2"));
        BOOST_CHECK( f.isPathAllowed("/image"));
        BOOST_CHECK(!f.isPathAllowed("/images"));
        BOOST_CHECK(!f.isPathAllowed("/images/"));
    }

    {
        RobotsFilter f("NOtSoSPECIAlBOt");
        f.loadRobotsTxt(TEST);

        BOOST_CHECK( f.isPathAllowed("/"));
        BOOST_CHECK(!f.isPathAllowed("/search"));
        BOOST_CHECK(!f.isPathAllowed("/searchstuff/foo"));
        BOOST_CHECK( f.isPathAllowed("/searchhistory/foo"));
        BOOST_CHECK( f.isPathAllowed("/banjos/index.html"));
        BOOST_CHECK(!f.isPathAllowed("/?anything"));
        BOOST_CHECK( f.isPathAllowed("/news?output=xhtml"));
        BOOST_CHECK( f.isPathAllowed("/news?output=xhtml2"));
        BOOST_CHECK(!f.isPathAllowed("/news?output=xhtml&page=2"));
        BOOST_CHECK( f.isPathAllowed("/image"));
        BOOST_CHECK(!f.isPathAllowed("/images"));
        BOOST_CHECK(!f.isPathAllowed("/images/"));
    }
}
