//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/EnvironmentVariable.cc $
//
// Created 2008/11/18
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <warp/EnvironmentVariable.h>
#include <ex/exception.h>
#include <stdlib.h>

using namespace warp;
using namespace ex;

//----------------------------------------------------------------------------
// EnvironmentVariable
//----------------------------------------------------------------------------
bool EnvironmentVariable::get(std::string const & name, std::string & value)
{
    if(char const * v = getenv(name.c_str()))
    {
        value = v;
        return true;
    }
    else
    {
        return false;
    }
}

void EnvironmentVariable::set(std::string const & name, std::string const & value)
{
    if(0 != setenv(name.c_str(), value.c_str(), 1))
    {
        raise<RuntimeError>("setenv (%s=%s) failed: %s",
                            name, value, getStdError());
    }
}

void EnvironmentVariable::clear(std::string const & name)
{
    if(0 != unsetenv(name.c_str()))
    {
        raise<RuntimeError>("clearenv (%s) failed: %s",
                            name, getStdError());
    }
}
