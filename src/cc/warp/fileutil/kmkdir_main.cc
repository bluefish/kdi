//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-11-08
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

#include <warp/fileutil/app_template.h>

namespace
{
    void makeParents(string const & path, bool verbose)
    {
        if(fs::path(path).empty() || fs::isDirectory(path))
            return;

        string parent = fs::resolve(path, "..");
        if(parent != path)
            makeParents(parent, verbose);

        if(fs::mkdir(path) && verbose)
            cerr << format("created directory '%s'\n") % path;
    }
}

//----------------------------------------------------------------------------
// appOptions
//----------------------------------------------------------------------------
void appOptions(OptionParser & op)
{
    using po::value;

    op.setUsage("%prog [options] <dir> [dir2 ...]");
    op.addOption("parents,p", "create parent directories if necessary");
    op.addOption("verbose,v", "be verbose");
}


//----------------------------------------------------------------------------
// appMain
//----------------------------------------------------------------------------
void appMain(OptionMap const & opt, ArgumentList const & args)
{
    bool parents = hasopt(opt, "parents");
    bool verbose = hasopt(opt, "verbose");

    for(ArgumentList::const_iterator ai = args.begin(); ai != args.end(); ++ai)
    {
        if(parents)
            makeParents(*ai, verbose);
        else
        {
            if(fs::mkdir(*ai) && verbose)
                cerr << format("created directory '%s'\n") % *ai;
        }
    }
}
