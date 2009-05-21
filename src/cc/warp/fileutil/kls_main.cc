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

namespace {

    enum {
        PERMS,
        INODE,
        USER,
        GROUP,
        SIZE,
        MTIME,
        NAME,

        N_FIELDS
    };

    enum {
        SHOW_SIZE   = 1,
        SHOW_HUMAN  = 2,
        SHOW_MTIME  = 4,
    };

    struct Result
    {
        string fields[N_FIELDS];

        string & operator[](int i) { return fields[i]; }
        string const & operator[](int i) const { return fields[i]; }

        bool operator<(Result const & o) const
        {
            return fields[NAME] < o.fields[NAME];
        }
    };

    void printResults(vector<Result> const & results)
    {
        BOOST_STATIC_ASSERT(N_FIELDS > 0);
        typedef vector<Result>::const_iterator iter_t;

        // Get field widths
        size_t widths[N_FIELDS];
        size_t totalWidth = 0;
        std::fill(widths, widths + N_FIELDS, 0);
        for(int i = 0; i < N_FIELDS; ++i)
        {
            for(iter_t ri = results.begin(); ri != results.end(); ++ri)
            {
                widths[i] = std::max(widths[i], (*ri)[i].length());
            }

            if(widths[i] > 0)
                totalWidth += widths[i] + 1;
        }
        if(totalWidth)
            --totalWidth;

        // Prepare format strings
        char buf[64];
        string patterns[N_FIELDS];
        for(int i = 0; i < N_FIELDS-1; ++i)
        {
            if(widths[i] > 0)
            {
                snprintf(buf, sizeof(buf), "%%%lus ", widths[i]);
                patterns[i] = buf;
            }
            else
            {
                patterns[i] = "%s";
            }
        }
        patterns[N_FIELDS-1] = "%s\n";

        for(iter_t ri = results.begin(); ri != results.end(); ++ri)
        {
            for(int i = 0; i < N_FIELDS; ++i)
            {
                printf(patterns[i].c_str(), (*ri)[i].c_str());
            }
        }
        if(!results.empty())
            printf("\n");
    }

    void appendResult(vector<Result> & results, string const & dirPath,
                      string const & filename, int flags)
    {
        //printf("append result: %s / %s\n", dirPath.c_str(), filename.c_str());

        char buf[256];

        time_t six_months_ago = 0;
        if(flags & MTIME)
            six_months_ago = time(0) - 86400 * 30 * 6;

        Result r;
        r[NAME] = filename;

        if(flags)
        {
            string filepath = fs::resolve(dirPath, filename);
            //printf("  filepath: %s\n", filepath.c_str());

            if((flags & SHOW_SIZE))
            {
                if(fs::isFile(filepath))
                {
                    size_t sz = fs::filesize(filepath);
                    if(flags & SHOW_HUMAN)
                    {
                        r[SIZE] = sizeString(sz);
                        boost::trim_right(r[SIZE]);
                    }
                    else
                    {
                        snprintf(buf, sizeof(buf), "%lu", sz);
                        r[SIZE] = buf;
                    }
                }
                else
                {
                    r[SIZE] = "<dir>";
                }
            }
            if(flags & SHOW_MTIME)
            {
                time_t mtime = fs::modificationTime(filepath).toSeconds();
                struct tm mtime_tm;
                localtime_r(&mtime, &mtime_tm);

                if(mtime < six_months_ago)
                    strftime(buf, sizeof(buf), "%b %e  %Y", &mtime_tm);
                else
                    strftime(buf, sizeof(buf), "%b %e %H:%M", &mtime_tm);
                r[MTIME] = buf;
            }
        }

        results.push_back(r);
    }

    void getResults(vector<Result> & results, string const & dirPath, int flags)
    {
        results.clear();

        DirPtr dir = Directory::open(dirPath);
        string entry;
        while(dir->read(entry))
        {
            appendResult(results, dirPath, entry, flags);
        }
    }
}

//----------------------------------------------------------------------------
// appOptions
//----------------------------------------------------------------------------
void appOptions(OptionParser & op)
{
    using po::value;

    op.setUsage("%prog [options] [file ...]");
    //op.addOption("recursive,r", "Recursively list directories");
    op.addOption("long,l", "Show long listing");
    op.addOption("human-readable,H", "Show human readable numbers");
}


//----------------------------------------------------------------------------
// appMain
//----------------------------------------------------------------------------
void appMain(OptionMap const & opt, ArgumentList const & args)
{
    vector<Result> results;

    int flags = 0;
    if(hasopt(opt, "long"))
        flags |= SHOW_SIZE | SHOW_MTIME;
    if(hasopt(opt, "human-readable"))
        flags |= SHOW_HUMAN;

    if(args.empty())
    {
        // Show CWD
        getResults(results, ".", flags);
        std::sort(results.begin(), results.end());
        printResults(results);
    }
    else
    {
        bool multi = args.size() > 1;

        // Show all files
        for(ArgumentList::const_iterator ai = args.begin();
            ai != args.end(); ++ai)
        {
            if(!fs::exists(*ai))
            {
                fprintf(stderr, "kls: %s: No such file or directory\n", ai->c_str());
                continue;
            }
            else if(fs::isDirectory(*ai))
                continue;

            appendResult(results, ".", *ai, flags);
        }
        std::sort(results.begin(), results.end());
        printResults(results);

        // Show all directories
        for(ArgumentList::const_iterator ai = args.begin();
            ai != args.end(); ++ai)
        {
            if(!fs::isDirectory(*ai))
                continue;

            if(multi)
                cout << *ai << ':' << endl;

            getResults(results, *ai, flags);
            std::sort(results.begin(), results.end());
            printResults(results);
        }
    }
}
