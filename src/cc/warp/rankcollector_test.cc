//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-07-23
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

#include "rankcollector.h"
#include "options.h"
#include "timer.h"
#include <iostream>
#include <vector>
#include <algorithm>
#include <boost/format.hpp>
#include <math.h>

using namespace warp;
using namespace ex;
using namespace std;
using boost::format;

namespace
{
    struct RandGen
    {
        int operator()() const { return rand(); }
    };
    struct AscendingGen
    {
        int i;
        AscendingGen() : i(rand()) {}
        int operator()() { return ++i; }
    };
    struct DescendingGen
    {
        int i;
        DescendingGen() : i(rand()) {}
        int operator()() { return --i; }
    };


    template <class Collector, class Gen>
    double testRank(Collector & collector, Gen & gen,
                    size_t n, bool doSort,
                    double runTime, bool verbose)
    {
        int dummySum = 7;

        CpuTimer ct;
        size_t runs = 0;
        for(;;)
        {
            collector.clear();

            for(size_t i = 0; i < n; ++i)
                collector.insert(gen());

            if(doSort)
                collector.sort();

            for(typename Collector::const_iterator i = collector.begin();
                i != collector.end(); ++i)
            {
                dummySum += *i;
            }
            
            if((++runs & 256) == 0 && ct.getElapsed() > runTime)
                break;
        }

        double dt = ct.getElapsed();
        if(verbose)
            cout << format("%d batches in %.3f seconds (%.3f batches/second)\n")
                % runs % dt % (runs / dt);

        return runs / dt;
    }

    template <class Gen>
    void testGrid(Gen & gen, double q, bool doSort, double runTime)
    {
        size_t const TOP_EXP = 27;
        
        cout << format("SelectionRank(K,K*Q) / HeapRank(K) performance ratio:\n");

        cout << format("(Q=%g)\t") % q;
        for(size_t ke = 0; ke <= TOP_EXP; ++ke)
            cout << format("K=2^%d\t") % ke;
        cout << endl;

        for(size_t ne = 0; ne <= TOP_EXP; ++ne)
        {
            size_t n(1 << ne);

            cout << format("N=2^%d\t") % ne;
            cout.flush();

            for(size_t ke = 0; ke <= ne; ++ke)
            {
                size_t k(1 << ke);
                size_t cap = size_t(ceil(k * q));

                PartitionRankCollector<int> c(k, cap);
                double n1 = testRank(c, gen, n, doSort, runTime, false);

                HeapRankCollector<int> hc(k);
                double n2 = testRank(hc, gen, n, doSort, runTime, false);

                cout << format("%.3f\t") % (n1 / n2);
                cout.flush();
            }

            cout << endl;
        }

        cout << endl;
    }

    template <class Gen>
    void doIt(Gen & gen, OptionMap const & opt)
    {
        // Interpret options
        size_t n = 1000;
        opt.get("size", n);
        size_t topk = 10;
        opt.get("topk", topk);
        double factor = 2.0;
        opt.get("factor", factor);
        bool doSort = hasopt(opt, "sort");
        bool doGrid = hasopt(opt, "grid");
        double runTime = 1.0;
        opt.get("time", runTime);

        // Go go go!
        if(doGrid)
            testGrid(gen, factor, doSort, runTime);
        else
        {
            PartitionRankCollector<int> sc(topk, size_t(topk * factor));
            cout << "PartitionRankCollector: ";
            cout.flush();
            double pt = testRank(sc, gen, n, doSort, runTime, true);
    
            HeapRankCollector<int> hc(topk);
            cout << "     HeapRankCollector: ";
            cout.flush();
            float ht = testRank(hc, gen, n, doSort, runTime, true);
            cout << format("                 ratio: %.3f\n") % (pt / ht);
        }

    }
}

int main(int ac, char ** av)
{
    // Define and parse command line options
    OptionParser op;
    {
        using namespace boost::program_options;
        op.addOption("size,n", value<size_t>()->default_value(1000),
                     "Total size of collection");
        op.addOption("topk,k", value<size_t>()->default_value(10),
                     "Collect top-ranked K items from total collection");
        op.addOption("factor,q", value<double>()->default_value(2.0),
                     "Memory factor to use for PartitionRankCollector");
        op.addOption("time,t", value<double>()->default_value(1.0),
                     "Time in seconds to run each collector");
        op.addOption("grid", "Do a performance comparison test");
        op.addOption("gen", value<string>()->default_value("rand"),
                     "Type of number generator to use");
        op.addOption("sort,s", "Sort output");
    }
    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    string t;
    if(!opt.get("gen", t))
        op.error("need --gen");

    if(t == "rand")
    {
        RandGen gen;
        doIt(gen, opt);
    }
    else if(t == "ascending")
    {
        AscendingGen gen;
        doIt(gen, opt);
    }
    else if(t == "descending")
    {
        DescendingGen gen;
        doIt(gen, opt);
    }
    else
        op.error("unknown --gen: " + t);

    return 0;
}

