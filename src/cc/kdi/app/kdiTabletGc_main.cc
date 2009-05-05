//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-10-31
// 
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the Free Software
// Foundation; either version 2 of the License, or any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
// details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#include <kdi/table.h>
#include <kdi/tablet/TabletGc.h>
#include <warp/fs.h>
#include <ex/exception.h>
#include <warp/options.h>
#include <string>
#include <vector>
#include <iostream>

using namespace warp;
using namespace ex;
using namespace kdi;
using namespace std;
    
int main(int ac, char ** av)
{
    OptionParser op("%prog [options] --meta=<meta-table> --data=<data-directory>");
    {
        using namespace boost::program_options;
        op.addOption("meta,m", value<string>(), "location of META table");
        op.addOption("data,d", value<string>(), "location of data directory");
        op.addOption("before,b", value<string>(), "consider only files before "
                     "(argument can be a timestamp or another file)");
        op.addOption("dryrun,n", "don't delete anything, just print");
        op.addOption("verbose,v", "be verbose");
    }

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    bool dryrun = hasopt(opt, "dryrun");
    bool verbose = hasopt(opt, "verbose");

    string metaTable;
    if(!opt.get("meta", metaTable))
        op.error("need --meta");
    else if(verbose)
        cerr << "Using META table: " << metaTable << endl;
    
    string dataDir;
    if(!opt.get("data", dataDir) || !fs::isDirectory(dataDir))
        op.error("need --data directory");
    else if(verbose)
        cerr << "Using data directory: " << dataDir << endl;
    
    string beforeStr;
    Timestamp beforeTime;
    if(opt.get("before", beforeStr))
    {
        if(fs::isFile(beforeStr))
        {
            beforeTime = Timestamp::fromSeconds(
                fs::modificationTime(beforeStr));
        }
        else
        {
            beforeTime = Timestamp::fromString(beforeStr);
        }
    }
    else
    {
        beforeTime = Timestamp::now();
    }
    if(verbose)
        cerr << "Filtering files after: " << beforeTime << endl;


    if(verbose)
        cerr << "Searching for garbage files..." << endl;

    // Build the garbage set
    vector<string> garbageFiles;
    using kdi::tablet::TabletGc;
    TabletGc::findTabletGarbage(dataDir, Table::open(metaTable), beforeTime,
                                garbageFiles);

    if(verbose)
        cerr << "Found " << garbageFiles.size() << " file(s):" << endl;

    // Print and/or delete each file in the garbage set
    for(vector<string>::const_iterator i = garbageFiles.begin();
        i != garbageFiles.end(); ++i)
    {
        if(dryrun)
        {
            cout << *i << endl;
            continue;
        }

        if(verbose)
            cerr << "Deleting: " << *i << endl;

        fs::remove(*i);
    }
}
