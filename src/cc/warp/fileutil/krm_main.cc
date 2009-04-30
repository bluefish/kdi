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
    void removeHelper(string const & path, bool recursive, bool verbose)
    {
        if(fs::isDirectory(path))
        {
            if(recursive)
            {
                DirPtr dir = Directory::open(path);
                string p;
                while(dir->readPath(p))
                    removeHelper(p, recursive, verbose);
            }
            if(!fs::isEmpty(path))
            {
                cerr << format("krm: %s: Directory not empty\n") % path;
                return;
            }
        }

        fs::remove(path);
        if(verbose)
            cerr << format("removed '%s'\n") % path;
    }
}

//----------------------------------------------------------------------------
// appOptions
//----------------------------------------------------------------------------
void appOptions(OptionParser & op)
{
    using po::value;

    op.setUsage("%prog [options] file|dir [...]");
    op.addOption("verbose,v", "be verbose");
    op.addOption("recursive,r", "recursively remove directories "
                 "and their contents");
}


//----------------------------------------------------------------------------
// appMain
//----------------------------------------------------------------------------
void appMain(OptionMap const & opt, ArgumentList const & args)
{
    bool verbose = hasopt(opt, "verbose");
    bool recursive = hasopt(opt, "recursive");

    for(ArgumentList::const_iterator ai = args.begin();
        ai != args.end(); ++ai)
    {
        if(fs::exists(*ai))
            removeHelper(*ai, recursive, verbose);
        else if(verbose)
            cerr << format("krm: cannot remove '%s': No such file "
                           "or directory") % *ai << endl;
    }
}
