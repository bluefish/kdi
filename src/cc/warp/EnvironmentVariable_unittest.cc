//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/EnvironmentVariable_unittest.cc $
//
// Created 2008/11/18
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <warp/EnvironmentVariable.h>
#include <unittest/main.h>
#include <stdlib.h>
#include <string>

using namespace warp;

namespace {
    
    class Setup
    {
    public:
        Setup()
        {
            setenv("EV_TEST_1", "foo", 1);
            unsetenv("EV_TEST_2");
        }
    };

    std::string env(char const * name)
    {
        std::string val;
        if(char const * v = getenv(name))
            val = v;
        else
            val = "NULL";
        return val;
    }
}

BOOST_AUTO_UNIT_TEST(env_setup)
{
    Setup s;
    BOOST_CHECK_EQUAL(env("EV_TEST_1"), "foo");
    BOOST_CHECK_EQUAL(env("EV_TEST_2"), "NULL");
}

BOOST_AUTO_UNIT_TEST(env_get)
{
    Setup s;
    std::string val = "unset";

    BOOST_CHECK_EQUAL(EnvironmentVariable::get("EV_TEST_1", val), true);
    BOOST_CHECK_EQUAL(val, "foo");
    
    val = "unset";
    BOOST_CHECK_EQUAL(EnvironmentVariable::get("EV_TEST_2", val), false);
    BOOST_CHECK_EQUAL(val, "unset");
};

BOOST_AUTO_UNIT_TEST(env_set)
{
    Setup s;

    BOOST_CHECK_EQUAL(env("EV_TEST_1"), "foo");
    BOOST_CHECK_EQUAL(env("EV_TEST_2"), "NULL");

    EnvironmentVariable::set("EV_TEST_1", "bar");
    BOOST_CHECK_EQUAL(env("EV_TEST_1"), "bar");
    BOOST_CHECK_EQUAL(env("EV_TEST_2"), "NULL");

    EnvironmentVariable::set("EV_TEST_2", "baz");
    BOOST_CHECK_EQUAL(env("EV_TEST_1"), "bar");
    BOOST_CHECK_EQUAL(env("EV_TEST_2"), "baz");

    EnvironmentVariable::set("EV_TEST_1", "quux");
    BOOST_CHECK_EQUAL(env("EV_TEST_1"), "quux");
    BOOST_CHECK_EQUAL(env("EV_TEST_2"), "baz");
};

BOOST_AUTO_UNIT_TEST(env_clear)
{
    Setup s;

    BOOST_CHECK_EQUAL(env("EV_TEST_1"), "foo");
    BOOST_CHECK_EQUAL(env("EV_TEST_2"), "NULL");

    EnvironmentVariable::clear("EV_TEST_1");
    BOOST_CHECK_EQUAL(env("EV_TEST_1"), "NULL");
    BOOST_CHECK_EQUAL(env("EV_TEST_2"), "NULL");

    EnvironmentVariable::clear("EV_TEST_2");
    BOOST_CHECK_EQUAL(env("EV_TEST_1"), "NULL");
    BOOST_CHECK_EQUAL(env("EV_TEST_2"), "NULL");

    EnvironmentVariable::set("EV_TEST_2", "something");
    EnvironmentVariable::clear("EV_TEST_2");
    BOOST_CHECK_EQUAL(env("EV_TEST_1"), "NULL");
    BOOST_CHECK_EQUAL(env("EV_TEST_2"), "NULL");
};

BOOST_AUTO_UNIT_TEST(env_scoped_set)
{
    Setup s;

    BOOST_CHECK_EQUAL(env("EV_TEST_1"), "foo");
    BOOST_CHECK_EQUAL(env("EV_TEST_2"), "NULL");

    // Set 1
    {
        EnvironmentVariable v("EV_TEST_1", "waka");
        
        BOOST_CHECK_EQUAL(env("EV_TEST_1"), "waka");
        BOOST_CHECK_EQUAL(env("EV_TEST_2"), "NULL");
    }

    BOOST_CHECK_EQUAL(env("EV_TEST_1"), "foo");
    BOOST_CHECK_EQUAL(env("EV_TEST_2"), "NULL");

    // Set 2
    {
        EnvironmentVariable v("EV_TEST_2", "taka");
        
        BOOST_CHECK_EQUAL(env("EV_TEST_1"), "foo");
        BOOST_CHECK_EQUAL(env("EV_TEST_2"), "taka");
    }

    BOOST_CHECK_EQUAL(env("EV_TEST_1"), "foo");
    BOOST_CHECK_EQUAL(env("EV_TEST_2"), "NULL");
}

BOOST_AUTO_UNIT_TEST(env_scoped_clear)
{
    Setup s;

    BOOST_CHECK_EQUAL(env("EV_TEST_1"), "foo");
    BOOST_CHECK_EQUAL(env("EV_TEST_2"), "NULL");

    // Clear 1
    {
        EnvironmentVariable v("EV_TEST_1");
        
        BOOST_CHECK_EQUAL(env("EV_TEST_1"), "NULL");
        BOOST_CHECK_EQUAL(env("EV_TEST_2"), "NULL");
    }

    BOOST_CHECK_EQUAL(env("EV_TEST_1"), "foo");
    BOOST_CHECK_EQUAL(env("EV_TEST_2"), "NULL");

    // Clear 2
    {
        EnvironmentVariable v("EV_TEST_2");
        
        BOOST_CHECK_EQUAL(env("EV_TEST_1"), "foo");
        BOOST_CHECK_EQUAL(env("EV_TEST_2"), "NULL");
    }

    BOOST_CHECK_EQUAL(env("EV_TEST_1"), "foo");
    BOOST_CHECK_EQUAL(env("EV_TEST_2"), "NULL");
}
