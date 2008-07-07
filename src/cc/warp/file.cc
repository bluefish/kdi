//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-08-13
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

#include <warp/file.h>
#include <warp/uri.h>
#include <warp/strhash.h>
#include <warp/hashmap.h>
#include <warp/perflog.h>
#include <warp/perffile.h>
#include <ex/exception.h>
#include <algorithm>

using namespace warp;
using namespace ex;
using namespace std;

//#include <iostream>
//#include <boost/format.hpp>
//using boost::format;

//----------------------------------------------------------------------------
// Default implementation
//----------------------------------------------------------------------------
size_t File::readline(char * dst, size_t sz, char delim)
{
    if(!sz)
        return 0;
    --sz;
    
    char * p = dst;
    for(char * end = p + sz; p != end; ++p)
    {
        if(!read(p, 1, 1))
            break;
        
        if(*p == delim)
        {
            ++p;
            break;
        }
    }

    *p = 0;
    return p - dst;
}

void File::flush()
{
}

string File::getName() const
{
    return string();
}


//----------------------------------------------------------------------------
// Internals
//----------------------------------------------------------------------------
namespace
{
    // File registration table type
    typedef HashMap<string, FileOpenFunc *, HsiehHash> table_t;

    struct Details
    {
        table_t regTable;
        FileOpenFunc * defaultFunc;
        PerformanceLog * log;
        
        static Details & get()
        {
            static Details d;
            return d;
        }

    private:
        Details() : defaultFunc(0), log(0) {}
    };


    // Get filesystem registration table
    table_t & getTable()
    {
        return Details::get().regTable;
    }

    // Get default filesystem mapping function (may be null)
    FileOpenFunc * & defaultFunc()
    {
        return Details::get().defaultFunc;
    }

    // Get performance log for monitoring files.  Will be null if
    // performance logging isn't enabled.
    PerformanceLog * getPerformanceLog()
    {
        return Details::get().log;
    }
}


//----------------------------------------------------------------------------
// Dispatch mechanism
//----------------------------------------------------------------------------
FilePtr File::open(string const & uriFn, int flags)
{
    //cerr << format("File::open(\"%s\", %d)\n") % uriFn % flags;

    switch(flags & O_ACCMODE)
    {
        case O_RDONLY:
        case O_WRONLY:
        case O_RDWR:
            break;
        default:
            raise<ValueError>("invalid open access mode for '%s': %d",
                              uriFn, flags & O_ACCMODE);
    }
    if(flags & ~(O_ACCMODE | O_APPEND | O_TRUNC | O_CREAT | O_EXCL))
        raise<ValueError>("invalid open flags for '%s': 0x%x", uriFn, flags);

    Uri uri(wrap(uriFn));
    FileOpenFunc * func = 0;

    if(uri.schemeDefined())
    {
        func = getTable().get(uri.scheme, 0);
        if(!func)
            func = getTable().get(uri.topScheme(), 0);
        if(!func)
            raise<RuntimeError>("unrecognized file scheme '%s': %s",
                                uri.scheme, uri);
    }
    else if(uriFn == "-")
    {
        return open("pipe:-", flags);
    }
    else
    {
        func = defaultFunc();
        if(!func)
            raise<RuntimeError>("no default file scheme: %s", uri);
    }

    // Open file
    FilePtr fp = (*func)(uriFn, flags);

    // Seek to fragment
    off_t offset;
    if(uri.fragment && parseInt(offset, uri.fragment))
    {
        if(offset < 0)
            fp->seek(offset, SEEK_END);
        else
            fp->seek(offset, SEEK_SET);
    }

    // Add performance monitoring wrapper over file if performance
    // logging is enabled
    if(PerformanceLog * log = getPerformanceLog())
    {
        FilePtr pfp(new PerformanceFile(fp, log));
        fp = pfp;
    }

    return fp;
}

void File::registerScheme(char const * scheme, FileOpenFunc * func)
{
    if(!scheme || !*scheme)
        raise<ValueError>("null scheme name");
    if(!func)
        raise<ValueError>("null handler for scheme '%1%'", scheme);

    getTable().set(scheme, func);
}

void File::setDefaultScheme(char const * scheme)
{
    if(!scheme)
        defaultFunc() = 0;
    else
    {
        FileOpenFunc * func = getTable().get(scheme, 0);
        if(!func)
            raise<RuntimeError>("unrecognized file scheme: %s", scheme);
        defaultFunc() = func;
    }
}


//----------------------------------------------------------------------------
// Performance monitoring hook
//----------------------------------------------------------------------------
namespace
{
    void startPerfLog(PerformanceLog * log)
    {
        Details::get().log = log;
    }

    void stopPerfLog(PerformanceLog * log)
    {
        Details::get().log = 0;
    }
}

#include <warp/init.h>
WARP_DEFINE_INIT(warp_file)
{
    PerformanceLog::getCommon().addHooks(&startPerfLog, &stopPerfLog);

    // Hack for static libraries -- get basic functionality
    WARP_CALL_INIT(warp_stdfile);
}
