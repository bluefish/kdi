//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-06-18
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#include <warp/fsutil.h>
#include <warp/strutil.h>
#include <warp/fs.h>
#include <warp/dir.h>

#include <algorithm>
#include <iostream>

using namespace warp;
using namespace std;


//----------------------------------------------------------------------------
// findFilesNumericSorted
//----------------------------------------------------------------------------
void warp::findFilesNumericSorted(string const & dirName,
                                  string const & ext,
                                  vector<string> & out)
{
    out.clear();
    DirPtr dir = Directory::open(dirName);

    // Find files in dir that match the extension
    vector<string> bases;
    string path;
    while(dir->readPath(path))
    {
        if(fs::isFile(path) && fs::extension(path) == ext)
            bases.push_back(fs::basename(path, true));
    }

    // Numeric sort files by basename
    std::sort(bases.begin(), bases.end(), PseudoNumericLt());

    // Add files to output list
    for(vector<string>::const_iterator bi = bases.begin();
        bi != bases.end(); ++bi)
    {
        out.push_back(fs::resolve(dir->getPath(), *bi + ext));
    }
}


//----------------------------------------------------------------------------
// findDirsNumericSorted
//----------------------------------------------------------------------------
void warp::findDirsNumericSorted(string const & dirName,
                                 vector<string> & out)
{
    out.clear();
    DirPtr dir = Directory::open(dirName);

    // Find subdirs in dir
    vector<string> subdirs;
    string path;
    while(dir->readPath(path))
    {
        if(fs::isDirectory(path))
            subdirs.push_back(fs::basename(path));
    }

    // Numeric sort subdirs
    std::sort(subdirs.begin(), subdirs.end(), PseudoNumericLt());

    // Add files to output list
    for(vector<string>::const_iterator di = subdirs.begin();
        di != subdirs.end(); ++di)
    {
        out.push_back(fs::resolve(dir->getPath(), *di));
    }
}

//----------------------------------------------------------------------------
// getExistingDirs
//----------------------------------------------------------------------------
void warp::getExistingDirs(string const & base,
                           vector<string> const & children,
                           vector<string> & out,
                           bool reportMissing)
{
    out.clear();

    // Filter base/{children} for existing files
    for(vector<string>::const_iterator ci = children.begin();
        ci != children.end(); ++ci)
    {
        string child = fs::resolve(base, *ci);
        if(fs::isDirectory(child))
            out.push_back(child);
        else if(reportMissing)
            cerr << "could not find directory, skipping: "
                 << child << endl;
    }
}

//----------------------------------------------------------------------------
// getExistingFiles
//----------------------------------------------------------------------------
void warp::getExistingFiles(string const & base,
                            vector<string> const & children,
                            string const & ext,
                            vector<string> & out,
                            bool reportMissing)
{
    out.clear();

    // Filter base/{children}.ext for existing files
    for(vector<string>::const_iterator ci = children.begin();
        ci != children.end(); ++ci)
    {
        string child = fs::resolve(base, *ci + ext);
        if(!fs::isFile(child))
            out.push_back(child);
        else if(reportMissing)
            cerr << "could not find file, skipping: "
                 << child << endl;
    }
}
