//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/oort/recordstream.cc#1 $
//
// Created 2006/08/18
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "recordstream.h"
#include "headers.h"
#include "fileio.h"
#include "recordbuffer.h"
#include "perfstream.h"

#include "warp/hashmap.h"
#include "warp/strhash.h"
#include "warp/file.h"
#include "warp/uri.h"
#include "warp/perflog.h"

using namespace oort;
using namespace warp;
using namespace ex;
using namespace std;

//#include <iostream>
//#include <boost/format.hpp>
//using boost::format;

namespace
{
    // Stream opener registration table type
    typedef HashMap<string, OpenStreamFunc *, HsiehHash> table_t;

    // Get stream opener registration table
    table_t & getTable()
    {
        static table_t tbl;
        return tbl;
    }

    PerformanceLog *& getPLog()
    {
        static PerformanceLog * plog = 0;
        return plog;
    }

    // Default file stream opener
    RecordStreamHandle openFileStream(string const & uriFn, int flags)
    {
        //cerr << format("openFileStream(\"%s\", %d)\n") % uriFn % flags;

        Uri uri(wrap(uriFn));

        // Old-style compatibility glue
        {
            // file:some/path/and/some/file.cdf:vega:4m
            // args:
            //  0: invokation name
            //  1: filename
            //  1: [spec=default]
            //  2: [alloc=common]  (for input)

            // Does the path section of the URI contain a colon?
            char const * colon = find(uri.path.begin(), uri.path.end(), ':');
            if(colon != uri.path.end())
            {
                // Path has a colon in it.  Construct a new-style URI
                // with a query string from the old argument.
                string altFn(uri.path.begin(), colon);
                altFn += "?spec=";
                char const * c2 = find(colon+1, uri.path.end(), ':');
                altFn.append(colon+1, c2);
                if(c2 != uri.path.end())
                {
                    altFn += "&alloc=";
                    altFn.append(c2+1, uri.path.end());
                }
                return openFileStream(altFn, flags);
            }
        }

        // Set default params
        HeaderSpec const * spec = VEGA_SPEC;
        size_t allocSz = 32<<10;
        
        // Get params
        for(UriQuery q(uri.query); q.hasAttr(); q.next())
        {
            if(q.key == "spec")
            {
                if(q.value == "vega")
                    spec = VEGA_SPEC;
                else if(q.value == "sirius")
                    spec = SIRIUS_SPEC;
                else
                    raise<RuntimeError>("unknown spec: '%1%'", q.value);
            }
            else if(q.key == "alloc")
            {
                allocSz = parseSize(str(q.value));
            }
        }

        //cerr << format("  spec: %s\n") % (spec == VEGA_SPEC ? "vega" :
        //                                  spec == SIRIUS_SPEC ? "sirius" :
        //                                  spec ? "null" : "unknown");
        //cerr << format("  alloc: %d\n") % allocSz;

        // Open file
        FilePtr fp = File::open(uriFn, flags);
        
        // Put a Record marshaling Stream on it
        RecordStreamHandle h;
        if((flags & O_ACCMODE) == O_RDONLY)
        {
            FileInput::alloc_t alloc(new RecordBufferAllocator(allocSz));
            h.reset(new FileInput(fp, spec, alloc));
        }
        else if((flags & O_ACCMODE) == O_WRONLY)
        {
            h.reset(new FileOutput(fp, spec));
        }
        else
            raise<NotImplementedError>("no O_RDWR File-Stream adapter: %s", uriFn);

        // Yay, return a stream
        return h;
    }
}

RecordStreamHandle oort::openStream(string const & uriFn, int flags)
{
    Uri uri(wrap(uriFn));

    //cerr << format("openStream(\"%s\", %d)\n") % uriFn % flags;
    
    // Try to find an appropriate opener scheme for the given URI
    table_t const & tbl = getTable();
    OpenStreamFunc * func = 0;
    if(uri.scheme)
    {
        //cerr << format("  scheme: %s\n") % uri.scheme;

        // Lookup scheme directly
        func = tbl.get(uri.scheme, 0);
        if(!func)
        {
            // Check to see if we have a scheme composition and lookup
            // by first part (e.g. split+kfs:...)
            char const * compEnd =
                find(uri.scheme.begin(), uri.scheme.end(), '+');

            if(compEnd != uri.scheme.end())
            {
                str_data_t headScheme(uri.scheme.begin(), compEnd);
                //cerr << format("  scheme: %s\n") % headScheme;
                func = tbl.get(headScheme, 0);
            }
        }
    }

    // If we don't have the scheme specifically registered for
    // streams, assume it's some kind of file scheme.
    if(!func)
    {
        //cerr << format("  default file scheme\n");
        func = &openFileStream;
    }


    // If performance logging is enabled, wrap stream in performance
    // logger adapter.
    if(PerformanceLog * log = getPLog())
    {
        RecordStreamHandle ps(
            new PerformanceRecordStream((*func)(uriFn, flags), uriFn, log));
        return ps;
    }
    else
    {
        // Open stream
        return (*func)(uriFn, flags);
    }
}

RecordStreamHandle oort::inputStream(std::string const & uriFn)
{
    return openStream(uriFn, O_RDONLY);
}

RecordStreamHandle oort::outputStream(std::string const & uriFn)
{
    return openStream(uriFn, O_WRONLY | O_CREAT | O_TRUNC);
}

RecordStreamHandle oort::appendStream(std::string const & uriFn)
{
    return openStream(uriFn, O_WRONLY | O_CREAT | O_APPEND);
}

void oort::registerScheme(char const * name, OpenStreamFunc * func)
{
    if(!name || !*name)
        raise<ValueError>("null scheme name");
    if(!func)
        raise<ValueError>("null handler for scheme '%1%'", name);

    getTable().set(name, func);
}

//----------------------------------------------------------------------------
// Performance logging hooks
//----------------------------------------------------------------------------
namespace
{
    void startHook(PerformanceLog * log)
    {
        getPLog() = log;
    }
    
    void stopHook(PerformanceLog * log)
    {
        getPLog() = 0;
    }
}

#include "warp/init.h"
WARP_DEFINE_INIT(oort_recordstream)
{
    PerformanceLog::getCommon().addHooks(&startHook, &stopHook);
}
