//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/tmp/gentable_test.cc#1 $
//
// Created 2007/10/22
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/table.h>
#include <kdi/synchronized_table.h>
#include <warp/options.h>
#include <boost/random/linear_congruential.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <string>
#include <vector>

using namespace kdi;
using namespace warp;
using namespace std;

std::string genData(uint64_t seed, size_t len)
{
    static char const ALPHABET[] = "abcdefghijklmnopqrstuvwxyz0123456789-";
    static size_t const ALPHABET_SZ = strlen(ALPHABET);
    
    boost::rand48 rng(seed);

    std::string result;
    for(size_t i = 0; i < len; ++i)
        result += ALPHABET[size_t(rng()) % ALPHABET_SZ];

    return result;
}

void genData(vector<string> & out, size_t len, uint64_t seed)
{
    out.resize(len);
    for(size_t i = 0; i < len; ++i)
        out[i] = genData(seed + i, 10);
}

template <class RNG>
string const & pick(vector<string> const & arr, RNG & rng)
{
    return arr[size_t(rng()) % arr.size()];
}

void fillTable(TablePtr const & tbl, size_t nCells, uint64_t seed,
               vector<string> const & rows,
               vector<string> const & columns,
               vector<string> const & values)
{
    boost::rand48 rng(seed);

    for(size_t i = 0; i < nCells; ++i)
    {
        tbl->set(pick(rows, rng),
                 pick(columns, rng),
                 rng(),
                 pick(values, rng));
    }
}

int main(int ac, char ** av)
{
    // Define options
    OptionParser op;
    {
        using namespace boost::program_options;
        op.addOption("table,t", value<string>(), "Destination table");
        op.addOption("ncells,n", value<size_t>(),
                     "Number of cells to generate (per thread)");
        op.addOption("nthreads,N", value<size_t>()->default_value(1),
                     "Number of generator threads");
        op.addOption("nrows,r", value<size_t>()->default_value(20),
                     "Number of distinct rows");
        op.addOption("ncolumns,c", value<size_t>()->default_value(20),
                     "Number of distinct columns");
        op.addOption("nvalues,v", value<size_t>()->default_value(20),
                     "Number of distinct values");
        op.addOption("buffer,b", value<size_t>()->default_value(0),
                     "Mutation buffer size");
    }

    // Parse options
    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    // Interpret options
    string tableName;
    if(!opt.get("table", tableName))
        op.error("need --table");

    size_t nCells = 0;
    if(!opt.get("ncells", nCells))
        op.error("need --ncells");

    size_t nThreads = 0;
    if(!opt.get("nthreads", nThreads))
        op.error("need --nthreads");

    size_t nRows = 0;
    if(!opt.get("nrows", nRows) || !nRows)
        op.error("need --nrows > 0");

    size_t nColumns = 0;
    if(!opt.get("ncolumns", nColumns) || !nColumns)
        op.error("need --ncolumns > 0");

    size_t nValues = 0;
    if(!opt.get("nvalues", nValues) || !nValues)
        op.error("need --nvalues > 0");

    size_t bufferSz = 0;
    opt.get("buffer", bufferSz);

    // Open table
    SyncTablePtr tbl = SynchronizedTable::make(
        Table::open(tableName)
        );

    // Generate data
    vector<string> rows;
    vector<string> columns;
    vector<string> values;
    cout << "Generating rows" << endl;
    genData(rows, nRows, 0);
    cout << "Generating columns" << endl;
    genData(columns, nColumns, nRows);
    cout << "Generating values" << endl;
    genData(values, nValues, nRows + nColumns);
    
    // Create threads
    vector< boost::shared_ptr< boost::thread > > threads;
    for(size_t i = 0; i < nThreads; ++i)
    {
        TablePtr t;
        if(bufferSz > 1)
            t = tbl->makeBuffer(bufferSz, bufferSz);
        else
            t = tbl;

        cout << "Starting generator thread " << (i+1) << endl;
        boost::shared_ptr<boost::thread> thread(
            new boost::thread(
                boost::bind(
                    &fillTable,
                    t,
                    nCells, i,
                    rows, columns, values
                    )
                )
            );

        threads.push_back(thread);
    }

    // Wait for threads
    for(size_t i = 0; i < nThreads; ++i)
    {
        cout << "Waiting on thread " << (i+1) << endl;

        threads[i]->join();
        threads[i].reset();
    }

    // Sync table before exiting
    tbl->sync();
}
