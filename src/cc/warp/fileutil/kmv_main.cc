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
#include <boost/algorithm/string.hpp>

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

    op.setUsage("%prog [options] <src1> [src2 ...] <dst>");
    op.addOption("noclobber,n", "do not overwrite existing files");
    op.addOption("nocomplain,N", "don't complain about not overwriting "
                 "existing files");
    op.addOption("no-target-directory,T", "treat dst as a regular file");
    op.addOption("parents,p", "create parent directory for target");
    op.addOption("verbose,v", "be verbose");
}


//----------------------------------------------------------------------------
// appMain
//----------------------------------------------------------------------------
void appMain(OptionMap const & opt, ArgumentList const & args)
{
    bool verbose = hasopt(opt, "verbose");
    bool noclobber = hasopt(opt, "noclobber");
    bool parent = hasopt(opt, "parents");
    bool nocomplain = hasopt(opt, "nocomplain");
    bool notargetdir = hasopt(opt, "no-target-directory");

    if(args.size() < 2)
        raise<OptionError>("need at least two arguments");

    if(args.size() > 2 && notargetdir)
        raise<OptionError>("multiple sources with --no-target-directory");

    string const & dst = args.back();

    if(parent)
    {
        string pdir;
        if(!notargetdir &&
           (args.size() > 2 || boost::ends_with(fs::path(dst), "/")))
            pdir = dst;
        else
            pdir = fs::resolve(dst, "..");

        makeParents(pdir, verbose);
    }

    if(args.size() > 2 && !fs::isDirectory(dst))
    {
        cerr << "kmv: target '" << dst << "' is not a directory" << endl;
        throw SystemExit(1);
    }

    if(!notargetdir && fs::isDirectory(dst))
    {
        for(size_t i = 0; i < args.size()-1; ++i)
        {
            string const & src = args[i];
            string target = fs::resolve(dst, fs::basename(src));

            try {
                fs::rename(src, target, !noclobber);
                if(verbose)
                    cerr << format("kmv: renamed '%s' to '%s'\n") % src % target;
            }
            catch(IOError const & ex) {
                if(noclobber && nocomplain && fs::exists(target))
                {
                    if(verbose)
                        cerr << format("kmv: not renaming '%s' to existing "
                                       "target '%s'\n") % src % target;
                }
                else
                    throw;
            }
        }
    }
    else
    {
        // Rename mode
        for(size_t i = 0; i < args.size()-1; ++i)
        {
            string const & src = args[i];

            try {
                fs::rename(src, dst, !noclobber);
                if(verbose)
                    cerr << format("kmv: renamed '%s' to '%s'\n") % src % dst;
            }
            catch(IOError const & ex) {
                if(noclobber && nocomplain && fs::exists(dst))
                {
                    if(verbose)
                        cerr << format("kmv: not renaming '%s' to existing "
                                       "target '%s'\n") % src % dst;
                }
                else
                    throw;
            }
        }
    }
}
