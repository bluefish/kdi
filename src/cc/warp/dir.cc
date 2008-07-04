//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-09-20
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

#include <warp/dir.h>
#include <warp/fs.h>
#include <warp/uri.h>
#include <warp/strutil.h>
#include <warp/strhash.h>
#include <warp/hashmap.h>
#include <ex/exception.h>
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
#include <warp/init.h>
WARP_DEFINE_INIT(warp_dir)
{
    // Get basic functionality
    WARP_CALL_INIT(warp_posixdir);
}
