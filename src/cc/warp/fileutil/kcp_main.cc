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
#include <warp/timer.h>
#include <boost/algorithm/string.hpp>

namespace
{
    enum {
        F_VERBOSE = 1,
        F_RECURSIVE = 2,
        F_NO_TARGET_DIR = 4,
        F_TIMING = 8,
        F_MAKE_PARENTS = 16,
        F_UPDATE = 32
    };


    EX_DECLARE_EXCEPTION(CopyError, RuntimeError);

    size_t copyFile(string const & src, string const & dst, int flags)
    {
        if(flags & F_VERBOSE)
            cerr << format("'%s' -> '%s'\n") % src % dst;

        if (flags & F_UPDATE) {
            if (fs::exists(dst) &&
                (fs::filesize(src) == fs::filesize(dst)) &&
                (fs::modificationTime(src) <= fs::modificationTime(dst))) {
                if (flags & F_VERBOSE) {
                    cerr << format ("skipping '%s' as it exists as '%s'\n") % src % dst;
                }
                return 0;
            }
        }

        FilePtr srcFp = File::input(src);
        FilePtr dstFp = File::output(dst);

        WallTimer wt;
        size_t bytesCopied = 0;
        char buf[32<<10];
        for(;;)
        {
            size_t readSz = srcFp->read(buf, sizeof(buf));
            if(!readSz)
                break;

            size_t writeSz = dstFp->write(buf, readSz);
            if(writeSz < readSz)
                raise<IOError>("short write (%s of %s bytes) to file %s",
                               writeSz, readSz, dst);

            bytesCopied += writeSz;
        }

        if((flags & F_VERBOSE) && (flags & F_TIMING))
        {
            double dt = wt.getElapsed();
            cerr << format("  %sB / %.3fs : %sB/s\n") % sizeString(bytesCopied)
                % dt % sizeString(size_t(bytesCopied / dt));
        }

        return bytesCopied;
    }

    size_t recursiveCopy(string const & src, string const & dst, int flags)
    {
        if(fs::isDirectory(src))
        {
            // Recursive copy source directory to target directory

            // Check to see if target directory exists
            if(!fs::exists(dst))
            {
                // Make target directory
                if(fs::mkdir(dst) && (flags & F_VERBOSE))
                    cerr << format("'%s' -> '%s'\n") % src % dst;
            }
            else if(!fs::isDirectory(dst))
            {
                // Cannot copy directory onto file
                raise<CopyError>("cannot overwrite non-directory '%s' with "
                                 "directory '%s'", dst, src);
            }

            // Copy src/* to dst/*
            size_t bytesCopied = 0;
            DirPtr dir = Directory::open(src);
            for(string ent; dir->read(ent);)
            {
                bytesCopied += recursiveCopy(fs::resolve(src, ent),
                                             fs::resolve(dst, ent),
                                             flags);
            }
            return bytesCopied;
        }
        else
        {
            // Copy non-directory to target

            if(fs::isDirectory(dst))
            {
                raise<CopyError>("cannot overwrite directory '%s' with "
                                 "non-directory '%s'", dst, src);
            }

            return copyFile(src, dst, flags);
        }
    }

    void makeParents(string const & path, int flags)
    {
        if(fs::path(path).empty() || fs::isDirectory(path))
            return;

        string parent = fs::resolve(path, "..");
        if(parent != path)
            makeParents(parent, flags);

        if(fs::mkdir(path) && (flags & F_VERBOSE))
            cerr << format("created directory '%s'\n") % path;
    }

    size_t doCopy(string const & src, string const & dst, int flags)
    {
        if(src != "-" && !fs::exists(src))
        {
            raise<CopyError>("cannot stat '%s': No such file or "
                             "directory", src);
        }

        // Skip directories if we're not doing a recursive copy
        if(!(flags & F_RECURSIVE) && fs::isDirectory(src))
        {
            raise<CopyError>("omitting directory '%s'", src);
        }

        // Figure out target
        if(!(flags & F_NO_TARGET_DIR) && fs::isDirectory(dst))
        {
            // Target is directory:
            //   if isDirectory(src):
            //      copy src/* to dst/src/*
            //   else:
            //      copy src to dst/src
            string base = fs::basename(src);
            if(base.empty())
                base = fs::basename(fs::dirname(src));
            return recursiveCopy(src, fs::resolve(dst, base), flags);
        }
        else
        {
            // Target is non-directory:
            //   if isDirectory(src):
            //      copy src/* to dst/*
            //   else:
            //      copy src to dst
            return recursiveCopy(src, dst, flags);
        }
    }
}

//----------------------------------------------------------------------------
// appOptions
//----------------------------------------------------------------------------
void appOptions(OptionParser & op)
{
    using po::value;

    op.setUsage("%prog [options] <src1> [src2 ...] <dst>");
    op.addOption("recursive,r", "copy directories recursively");
    op.addOption("no-target-directory,T", "treat dst as a regular file");
    op.addOption("update,u", "copy only if src is newer than dst or dst is missing");
    op.addOption("verbose,v", "be verbose");
    op.addOption("timing,t", "report performance timing");
    op.addOption("parents,p", "make target parent directories");
}


//----------------------------------------------------------------------------
// appMain
//----------------------------------------------------------------------------
void appMain(OptionMap const & opt, ArgumentList const & args)
{
    int flags = 0;
    if(hasopt(opt, "verbose"))
        flags |= F_VERBOSE;

    if(hasopt(opt, "recursive"))
        flags |= F_RECURSIVE;

    if(hasopt(opt, "no-target-directory"))
        flags |= F_NO_TARGET_DIR;

    if(hasopt(opt, "timing"))
        flags |= F_TIMING;

    if(hasopt(opt, "parents"))
        flags |= F_MAKE_PARENTS;

    if(hasopt(opt, "update"))
        flags |= F_UPDATE;

    if(args.size() < 2)
        raise<OptionError>("need at least two arguments");

    if(args.size() > 2 && (flags & F_NO_TARGET_DIR))
        raise<OptionError>("multiple sources with --no-target-directory");

    bool hadErrors = false;

    WallTimer wt;
    size_t bytesCopied = 0;
    string const & dst = args.back();

    if(flags & F_MAKE_PARENTS)
    {
        string parent;
        if(!(flags & F_NO_TARGET_DIR) &&
           (args.size() > 2 || boost::ends_with(fs::path(dst), "/")))
            parent = dst;
        else
            parent = fs::resolve(dst, "..");
        makeParents(parent, flags);
    }

    if(args.size() > 2 && !fs::isDirectory(dst))
    {
        cerr << "kcp: target '" << dst << "' is not a directory" << endl;
        throw SystemExit(1);
    }

    for(size_t i = 0; i < args.size()-1; ++i)
    {
        try {
            bytesCopied += doCopy(args[i], dst, flags);
        }
        catch(CopyError const & ex) {
            cerr << "kcp: " << ex.what() << endl;
            hadErrors = true;
        }
    }

    if(flags & F_TIMING)
    {
        double dt = wt.getElapsed();
        cerr << format("Copied %sB in %.3fs : %sB/s\n") % sizeString(bytesCopied)
            % dt % sizeString(size_t(bytesCopied / dt));
    }

    if(hadErrors)
        throw SystemExit(1);
}
