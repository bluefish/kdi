//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/dir.cc#2 $
//
// Created 2006/09/20
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "dir.h"
#include "fs.h"
#include "uri.h"
#include "strutil.h"
#include "strhash.h"
#include "hashmap.h"
#include "ex/exception.h"
#include <algorithm>

using namespace warp;
using namespace ex;
using std::string;

//----------------------------------------------------------------------------
// Internals
//----------------------------------------------------------------------------
namespace
{
    // Directory registration table type
    typedef HashMap<string, DirOpenFunc *, HsiehHash> table_t;

    struct Details
    {
        table_t regTable;
        DirOpenFunc * defaultFunc;
        
        static Details & get()
        {
            static Details d;
            return d;
        }

    private:
        Details() : defaultFunc(0) {}
    };


    // Get filesystem registration table
    table_t & getTable()
    {
        return Details::get().regTable;
    }

    // Get default directory open function (may be null)
    DirOpenFunc * & defaultFunc()
    {
        return Details::get().defaultFunc;
    }
}

//----------------------------------------------------------------------------
// Registration and dispatch
//----------------------------------------------------------------------------
DirPtr Directory::open(string const & uriStr)
{
    Uri uri(wrap(uriStr));
    DirOpenFunc * func = 0;

    if(uri.schemeDefined())
    {
        func = getTable().get(uri.scheme, 0);
        if(!func)
            func = getTable().get(uri.topScheme(), 0);
        if(!func)
            raise<RuntimeError>("unrecognized directory scheme '%s': %s",
                                uri.scheme, uri);
    }
    else
    {
        func = defaultFunc();
        if(!func)
            raise<RuntimeError>("no default directory scheme: %s", uri);
    }

    return (*func)(uriStr);
}

void Directory::registerScheme(char const * scheme, DirOpenFunc * func)
{
    if(!scheme || !*scheme)
        raise<ValueError>("null scheme name");
    if(!func)
        raise<ValueError>("null handler for scheme '%1%'", scheme);

    getTable().set(scheme, func);
}

void Directory::setDefaultScheme(char const * scheme)
{
    if(!scheme)
        defaultFunc() = 0;
    else
    {
        DirOpenFunc * func = getTable().get(scheme, 0);
        if(!func)
            raise<RuntimeError>("unrecognized directory scheme: %s", scheme);
        defaultFunc() = func;
    }
}

//----------------------------------------------------------------------------
// Directory
//----------------------------------------------------------------------------
bool Directory::readPath(std::string & dst)
{
    string entry;
    if(!read(entry))
        return false;
    
    dst = fs::resolve(getPath(), entry);
    return true;
}


//----------------------------------------------------------------------------
// Hack for static libraries
//----------------------------------------------------------------------------
#include "init.h"
WARP_DEFINE_INIT(warp_dir)
{
    // Get basic functionality
    WARP_CALL_INIT(warp_posixdir);
}
