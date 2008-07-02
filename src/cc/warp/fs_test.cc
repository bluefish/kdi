//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/fs_test.cc#1 $
//
// Created 2007/02/24
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "fs.h"
#include "options.h"
#include "strutil.h"
#include "init.h"
#include "ex/exception.h"
#include <iostream>
#include <boost/format.hpp>

using namespace warp;
using namespace ex;
using namespace std;
using boost::format;

namespace
{
    void testGlob(ArgumentList const & args)
    {
        raise<NotImplementedError>("file globbing not ready");

// file globbing disabled
#if 0
        for(ArgumentList::const_iterator ai = args.begin();
            ai != args.end(); ++ai)
        {
            cout << format("glob: %s\n") % reprString(wrap(*ai));

            vector<string> r = fs::glob(*ai);
            for(vector<string>::const_iterator ri = r.begin();
                ri != r.end(); ++ri)
            {
                cout << format("  match: %s\n") % reprString(wrap(*ri));
            }
        }

#endif
    }
}

int main(int ac, char ** av)
{
    using boost::program_options::value;
    OptionParser op;
    op.addOption("glob,g", "Test file globbing");
    
    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    initModule();

    if(hasopt(opt, "glob"))
        testGlob(args);

    return 0;
}
