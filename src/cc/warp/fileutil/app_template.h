//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-02-11
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

#ifndef WARP_FILEUTIL_APP_TEMPLATE_H
#define WARP_FILEUTIL_APP_TEMPLATE_H

#include <warp/options.h>
#include <warp/fs.h>
#include <warp/file.h>
#include <warp/dir.h>
#include <warp/strutil.h>
#include <ex/exception.h>

#include <string>
#include <vector>
#include <boost/format.hpp>

using namespace warp;
using namespace ex;
using namespace std;

namespace po=boost::program_options;
using boost::format;


//----------------------------------------------------------------------------
// Implement these:
//----------------------------------------------------------------------------
void appOptions(OptionParser & op);
void appMain(OptionMap const & opt, ArgumentList const & args);


//----------------------------------------------------------------------------
// main
//----------------------------------------------------------------------------
int main(int ac, char ** av)
{
    OptionParser op;
    try {
        appOptions(op);

        OptionMap opt;
        ArgumentList args;
        op.parse(ac, av, opt, args);

        appMain(opt, args);
    }
    catch(SystemExit const & ex) {
        return ex.exitStatus();
    }
    catch(OptionError const & ex) {
        op.error(ex.what(), false);
        return 2;
    }
    catch(std::exception const & ex) {
        cerr << ex.what() << endl;
        return 1;
    }
    return 0;
}

#endif // WARP_FILEUTIL_APP_TEMPLATE_H
