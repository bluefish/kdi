//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-14
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

#include <kdi/table.h>
#include <warp/options.h>
#include <warp/strutil.h>
#include <warp/timer.h>
#include <warp/log.h>

#include <boost/format.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>

#include <cmath>
#include <string>
#include <iostream>
#include <unistd.h>
#include <time.h>

using namespace kdi;
using namespace warp;
using namespace std;
using boost::format;

//----------------------------------------------------------------------------
// RandomStringGenerator
//----------------------------------------------------------------------------
class RandomStringGenerator
{
    size_t len;

public:
    explicit RandomStringGenerator(size_t len) :
        len(len) {}

    std::string operator()() const
    {
        static char const LETTERS[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        static size_t const N_LETTERS = strlen(LETTERS);
        
        size_t const MAX = size_t(RAND_MAX) + 1;
        
        std::string s;
        for(size_t i = 0; i < len; ++i)
            s += LETTERS[size_t(rand()) * N_LETTERS / MAX];
        return s;
    }
};


//----------------------------------------------------------------------------
// TestSet
//----------------------------------------------------------------------------
struct TestSet
{
    vector<string> rows;
    vector<string> columns;
    vector<int64_t> times;
    vector<string> values;
    size_t nCells;

    explicit TestSet(size_t nCells) :
        nCells(nCells)
    {
        rows.resize(size_t(sqrt(nCells) + 1));
        columns.resize(nCells / rows.size() + 1);

        std::generate(rows.begin(), rows.end(), RandomStringGenerator(10));
        std::generate(columns.begin(), columns.end(), RandomStringGenerator(10));

        times.push_back(time(0));
        values.push_back((format("test %s") % getpid()).str());
    }
};

//----------------------------------------------------------------------------
// writeTest
//----------------------------------------------------------------------------
pair<size_t, double> writeTest(string const & uri, TestSet const & testSet)
{
    TablePtr table = Table::open(uri);
    WallTimer timer;
    size_t n = 0;
    for(size_t r = 0; r < testSet.rows.size(); ++r)
    {
        for(size_t c = 0; c < testSet.columns.size(); ++c)
        {
            for(size_t t = 0; t < testSet.times.size(); ++t)
            {
                for(size_t v = 0; v < testSet.values.size(); ++v)
                {
                    table->set(testSet.rows[r],
                               testSet.columns[c],
                               testSet.times[t],
                               testSet.values[v]);

                    if(++n >= testSet.nCells)
                        goto done;
                }
            }
        }
    }

  done:
    table->sync();

    return make_pair(n, timer.getElapsed());
}

//----------------------------------------------------------------------------
// scanTest
//----------------------------------------------------------------------------
pair<size_t, double> scanTest(string const & uri)
{
    TablePtr table = Table::open(uri);
    WallTimer timer;
    size_t n = 0;

    CellStreamPtr scan = table->scan();
    Cell x;
    while(scan->get(x))
        ++n;

    return make_pair(n, timer.getElapsed());
}

//----------------------------------------------------------------------------
// threadTest
//----------------------------------------------------------------------------
void threadTest(std::string const & tableUri, size_t nCells, size_t id)
{
    log("%d: Generating test set", id);

    TestSet testSet(nCells);

    log("%d: Writing to %s: ", id, tableUri);
        
    pair<size_t, double> r = writeTest(tableUri, testSet);

    log("%d: %s cells in %ss (%.3f cells/s)",
        id, r.first, r.second, r.first / r.second);

    log("%d: Scanning from %s: ", id, tableUri);
    
    r = scanTest(tableUri);

    log("%d: %s cells in %ss (%.3f cells/s)",
        id, r.first, r.second, r.first / r.second);
}

void threadEntry(std::string const & tableUri, size_t nCells, size_t id)
{
    try {
        threadTest(tableUri, nCells, id);
    }
    catch(std::exception const & ex) {
        log("%d: unhandled exception: %s", id, ex.what());
    }
    catch(...) {
        log("%d: unhandled exception", id);
    }
}


//----------------------------------------------------------------------------
// main
//----------------------------------------------------------------------------
int main(int ac, char ** av)
{
    OptionParser op("%prog [options] <table-URI>");
    {
        using namespace boost::program_options;
        op.addOption("ncells,n", value<string>()->default_value("10k"),
                     "Number of cells to insert");
        op.addOption("nthreads,t", value<size_t>()->default_value(1),
                     "Number of threads to run");
    }

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    size_t nCells = 1;
    size_t nThreads = 1;
    string arg;
    if(opt.get("ncells", arg))
        nCells = parseSize(arg);
    opt.get("nthreads", nThreads);

    for(ArgumentList::const_iterator ai = args.begin(); ai != args.end(); ++ai)
    {
        boost::thread_group group;

        // Start all threads
        for(size_t i = 0; i < nThreads; ++i)
        {
            group.create_thread(
                boost::bind(
                    &threadEntry,
                    boost::cref(*ai), nCells, i
                    )
                );
        }
        
        // Join all threads
        group.join_all();
    }

    return 0;
}
