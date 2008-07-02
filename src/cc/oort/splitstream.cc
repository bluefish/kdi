//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/oort/splitstream.cc#1 $
//
// Created 2006/08/18
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "recordstream.h"
#include "partmgr.h"

#include "warp/uri.h"
#include "warp/util.h"
#include "warp/file.h"
#include "warp/fs.h"

#include "ex/exception.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include <string>
#include <algorithm>

using namespace oort;
using namespace warp;
using namespace ex;
using namespace std;

using namespace boost::algorithm;

//#include <iostream>
//#include <boost/format.hpp>
//using boost::format;

namespace
{
#if 0
}
#endif

//----------------------------------------------------------------------------
// Splitter
//----------------------------------------------------------------------------
class Splitter : public RecordStream
{
    RecordStreamHandle output;
    string fnTemplate;
    Partitioner partFunc;
    
    uint32_t part;
    uint32_t seq;

    void openNext()
    {
        using boost::lexical_cast;

        // Close current output
        output.reset();

        // Update sequence number
        ++seq;

        // Do variable substitution in path
        Uri uri(wrap(fnTemplate));
        string outPath = str(uri.path);
        replace_all(outPath, "@part", lexical_cast<string>(part));
        replace_all(outPath, "@seq", lexical_cast<string>(seq));
        string outUri(uri.begin(), uri.path.begin());
        outUri.append(outPath);
        outUri.append(uri.path.end(), uri.end());

        // Make directories
        fs::makedirs(fs::resolve(outUri, ".."));

        // Open output stream
        output = outputStream(outUri);
    }

public:
    Splitter(string const & fnTemplate, int nParts) :
        fnTemplate(fnTemplate), partFunc(nParts), part(0), seq(0)
    {
    }
    
    void put(Record const & x)
    {
        // Get partition for record
        uint32_t xPart = partFunc(x);

        // Open a new output stream if necessary
        if(!output || xPart != part)
        {
            part = xPart;
            openNext();
        }

        // Forward record to output
        output->put(x);
    }

    void flush()
    {
        // Close current output
        output.reset();
    }

    string getName() const
    {
        if(output)
            return "split:"+output->getName();
        else
            return "split";
    }
};


//----------------------------------------------------------------------------
// openSplitStream
//----------------------------------------------------------------------------
RecordStreamHandle openSplitStream(string const & uriFn, int flags)
{
    //cerr << format("openSplitStream(\"%s\", %d)\n") % uriFn % flags;

    // Only allow output split files
    if((flags & O_ACCMODE) != O_WRONLY)
        raise<RuntimeError>("split type is only for output");

    Uri uri(wrap(uriFn));

    // Old-style compatibility glue
    {
        // split:wds/url:1024:2033.17.0
        // args:
        //  0: invokation name
        //  1: stream name base
        //  2: partition count
        //  3: instance id

        // Does the path section of the URI contain a colon?
        char const * colon = find(uri.path.begin(), uri.path.end(), ':');
        if(colon != uri.path.end())
        {
            // Path has a colon in it.  Construct a new-style URI
            // with a query string from the old argument.

            char const * c2 = find(colon+1, uri.path.end(), ':');
            if(c2 == uri.path.end())
                raise<RuntimeError>("old-split needs 3 arguments: "
                                    "stream:nParts:id");

            string altFn(uri.path.begin(), colon);
            altFn += "/@part/";
            altFn.append(c2+1, uri.path.end());
            altFn += "/@seq.cdf?n=";
            altFn.append(colon+1, c2);
            return openSplitStream(altFn, flags);
        }
    }

    // Verify presence of placeholders
    if(!contains(uri.path, "@part") || !contains(uri.path, "@seq"))
        raise<RuntimeError>("split path should contain @part and "
                            "@seq placeholders");

    // Get number of partitions
    int nParts = -1;
    for(UriQuery q(uri.query); q.hasAttr(); q.next())
    {
        if(q.key == "n")
        {
            nParts = parseInt<int>(q.value);
            if(nParts <= 0)
                raise<RuntimeError>("split needs positive partition count, "
                                    "got: n=%1%", q.value);
            break;
        }
    }
    if(nParts == -1)
        raise<RuntimeError>("split need partition count (e.g. n=256)");

    // Supposing we got here using a composition scheme (for example,
    // split+kfs:///path?n=256), strip our part for the opening the
    // derived stream.
    string fnTemplate = str(uri.popScheme());

    // Make Splitter stream
    RecordStreamHandle h(new Splitter(fnTemplate, nParts));
    
    // Yay, return new stream
    return h;
}


#if 0
namespace
{
#endif
}

#include "warp/init.h"
WARP_DEFINE_INIT(oort_splitstream)
{
    registerScheme("split", &openSplitStream);
}
