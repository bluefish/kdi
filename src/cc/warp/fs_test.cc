//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-02-24
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

#include <warp/fs.h>
#include <warp/options.h>
#include <warp/strutil.h>
#include <warp/init.h>
#include <ex/exception.h>
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
