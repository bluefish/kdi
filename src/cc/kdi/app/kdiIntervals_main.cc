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
// cutPoint
//----------------------------------------------------------------------------
IntervalPoint<string>
cutPoint(IntervalPoint<string> const & x, size_t cutLength,
         bool useFields, string const & fieldDelimiter,
         bool includeTrailing)
{
    if(x.isInfinite())
        return x;

    string::size_type len = cutLength;

    if(useFields)
    {
        string::size_type begin = 0;
        string::size_type end = 0;

        for(size_t i = 0; i < cutLength; ++i)
        {
            begin = x.getValue().find(fieldDelimiter, end);
            if(begin != string::npos)
                end = begin + fieldDelimiter.size();
            else
            {
                end = string::npos;
                break;
            }
        }

        len = includeTrailing ? end : begin;
    }

    return IntervalPoint<string>(x.getValue().substr(0, len),
                                 x.isLowerBound() ?
                                 PT_INCLUSIVE_LOWER_BOUND :
                                 PT_EXCLUSIVE_UPPER_BOUND);
}


//----------------------------------------------------------------------------
// cutInterval
//----------------------------------------------------------------------------
Interval<string> cutInterval(Interval<string> const & x, size_t cutLength,
                             bool useFields, string const & fieldDelimiter,
                             bool includeTrailing)
{
    return Interval<string>(
        cutPoint(x.getLowerBound(), cutLength, useFields,
                 fieldDelimiter, includeTrailing),
        cutPoint(x.getUpperBound(), cutLength, useFields,
                 fieldDelimiter, includeTrailing));
}


//----------------------------------------------------------------------------
// main
//----------------------------------------------------------------------------
int main(int ac, char ** av)
{
    OptionParser op("%prog [options] <table> ...");
    {
        using namespace boost::program_options;
        op.addOption("verbose,v", "Be verbose");
        op.addOption("partition,p", value<size_t>()->default_value(0),
                     "Merge intervals into N partitions");
        op.addOption("cut,c", value<size_t>(),
                     "Cut predicate strings after first N chars/fields");
        op.addOption("delimiter,d", value<string>(),
                     "Field delimiter to use with --cut");
        op.addOption("trailing,t", "Include last delimiter in cut string");
    }

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    bool verbose = hasopt(opt, "verbose");

    size_t nPartitions = 0;
    opt.get("partition", nPartitions);
    typedef vector< pair<RowInterval,size_t> > interval_vec;
    interval_vec intervals;
    bool partition = nPartitions > 0;

    size_t cutLength(-1);
    bool useCut = opt.get("cut", cutLength);
    string fieldDelimiter;
    bool useFields = opt.get("delimiter", fieldDelimiter);
    bool includeTrailing = hasopt(opt, "trailing");

    RowInterval lastInterval;

    for(ArgumentList::const_iterator ai = args.begin(); ai != args.end(); ++ai)
    {
        if(verbose)
            cerr << "scanning: " << *ai << endl;

        RowIntervalStreamPtr scan = Table::open(*ai)->scanIntervals();
        RowInterval x;
        size_t nIntervals = 0;
        size_t nCutCombined = 0;
        while(scan->get(x))
        {
            ++nCutCombined;
            
            //cout << "GOT: " << x << endl;

            if(useCut)
            {
                x = RowInterval(
                    cutInterval(x, cutLength, useFields,
                                fieldDelimiter, includeTrailing));

                //cout << "CUT: " << x << endl;

                if(x.isEmpty() ||
                   lastInterval.contains(static_cast<Interval<string>&>(x)))
                {
                    //cout << "SKIP" << endl;
                    continue;
                }
            }

            //cout << "KEEP" << endl;

            if(partition)
                intervals.push_back(make_pair(x,nCutCombined));
            else
                cout << x << endl;

            nIntervals += nCutCombined;
            nCutCombined = 0;
            lastInterval = x;
        }

        if(verbose)
            cerr << nIntervals << " interval(s) in table" << endl;

        if(partition)
        {
            if(verbose)
                cerr << "Partitioning table " << nPartitions << " ways" << endl;

            int64_t i = 0;
            interval_vec::const_iterator lb = intervals.begin();
            for(interval_vec::const_iterator ub = intervals.begin();
                ub != intervals.end(); ++ub)
            {
                //cout << "IVAL: " << ub->second << " " << ub->first << endl;

                i += nPartitions * ub->second;
                if(i >= (int64_t)nIntervals)
                {
                    //cout << "LB: " << (lb - intervals.begin()) << "  UB: " << (ub - intervals.begin()) << endl;

                    cout << RowInterval(
                        lb->first.getLowerBound(),
                        ub->first.getUpperBound())
                         << endl;

                    i -= nIntervals;
                    lb = ub+1;
                }
            }
        }
    }

    return 0;
}
