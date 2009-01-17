//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-10-28
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
#include <kdi/RowInterval.h>
#include <warp/options.h>
#include <iostream>

using namespace kdi;
using namespace warp;
using namespace std;

//----------------------------------------------------------------------------
// main
//----------------------------------------------------------------------------
int main(int ac, char ** av)
{
    OptionParser op("%prog [options] <table> ...");
    {
        using namespace boost::program_options;
        op.addOption("verbose,v", "Be verbose");
        op.addOption("partition,p", value<unsigned int>()->default_value(0),
            "merge intervals into n partitions");
    }

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    bool verbose = hasopt(opt, "verbose");

    unsigned nPartitions = 0;
    opt.get("partition", nPartitions);
    vector<RowInterval> intervals;
    bool partition = nPartitions > 0;
    
    for(ArgumentList::const_iterator ai = args.begin(); ai != args.end(); ++ai)
    {
        if(verbose)
            cerr << "scanning: " << *ai << endl;

        RowIntervalStreamPtr scan = Table::open(*ai)->scanIntervals();
        RowInterval x;
        size_t nIntervals = 0;
        while(scan->get(x))
        {
            ++nIntervals;
            
            if(partition) {
                intervals.push_back(x);
            } else {
                cout << x << endl;
            }
        }

        if(verbose)
            cerr << nIntervals << " interval(s) in table" << endl;

        if(partition) {
            if(verbose)
                cerr << "Partitioning table " << nPartitions << " ways" << endl;

            size_t step_size = intervals.size() / nPartitions;
            if(step_size == 0) 
                step_size = 1;

            for(size_t i = 0; i < intervals.size(); i += step_size) {
                size_t j = i + step_size;
                if(j >= intervals.size())
                    j = intervals.size()-1;

                RowInterval::point_t lower = intervals[i].getLowerBound();
                RowInterval::point_t upper = intervals[j].getUpperBound();
                RowInterval p(lower, upper);
                cout << p << endl;
            }

            intervals.clear();
        }
    }

    return 0;
}
