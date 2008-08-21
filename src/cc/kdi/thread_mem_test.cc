//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/thread_mem_test.cc $
//
// Created 2008/08/20
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/table.h>
#include <warp/options.h>
#include <warp/strutil.h>
#include <warp/syncqueue.h>
#include <boost/format.hpp>
#include <stdlib.h>
#include <string>
#include <iostream>
#include <boost/thread.hpp>
#include <boost/bind.hpp>

using namespace kdi;
using namespace warp;
using namespace std;
using boost::format;

namespace
{
    void rstring(std::string & r, size_t len)
    {
        r.resize(len);
        uint32_t s = rand();
        for(size_t i = 0; i < len; ++i)
        {
            r[i] = 'a' + (s % 26);
            s = (s << 3) + (s >> 5) + 17;
        }
    }

    void producer(size_t nCells, SyncQueue<TablePtr> & queue)
    {
        for(;;)
        {
            TablePtr tbl = Table::open("mem:");
    
            uint32_t r = rand();
            uint32_t c = rand();
            uint32_t t = rand();
            uint32_t v = rand();
    
            string row;
            string col;
            string val;

            size_t nBytes = 0;
            for(size_t i = 0; i < nCells; ++i)
            {
                rstring(row, 10 + (r % 10));
                rstring(col, 10 + (c % 10));
                rstring(val, 50 + (v % 200));

                tbl->set(row, col, t, val);

                r = (r << 3) + (r >> 5) + 17;
                c = (c << 3) + (c >> 5) + 17;
                t = (t << 3) + (t >> 5) + 17;
                v = (v << 3) + (v >> 5) + 17;

                nBytes += row.size() + col.size() + val.size();
            }

            cout << format("Created %s cells (%sB)\n")
                % sizeString(nCells) % sizeString(nBytes);

            queue.push(tbl);
        }
    }

    void consumer(SyncQueue<TablePtr> & queue)
    {
        TablePtr tbl;
        while(queue.pop(tbl))
            tbl.reset();
    }
}

int main(int ac, char ** av)
{
    OptionParser op;
    {
        using namespace boost::program_options;
        op.addOption("ncells,n", value<string>(), "number of cells to generate");
        op.addOption("nconsumers,c", value<size_t>()->default_value(1),
                     "number of consumer threads");
        op.addOption("nproducers,p", value<size_t>()->default_value(1),
                     "number of producer threads");
        op.addOption("depth,d", value<size_t>()->default_value(4),
                     "maximum size of queue");
    }

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    string arg;

    size_t nCells = 0;
    if(opt.get("ncells", arg))
        nCells = parseSize(arg);

    size_t nProducers = 1;
    opt.get("nproducers", nProducers);

    size_t nConsumers = 1;
    opt.get("nconsumers", nConsumers);

    size_t depth = 4;
    opt.get("depth", depth);

    SyncQueue<TablePtr> queue(depth);
    boost::thread_group threads;

    for(size_t i = 0; i < nConsumers; ++i)
    {
        threads.create_thread(
            boost::bind(
                &consumer,
                boost::ref(queue)
                )
            );
    }

    for(size_t i = 0; i < nProducers; ++i)
    {
        threads.create_thread(
            boost::bind(
                &producer,
                nCells,
                boost::ref(queue)
                )
            );
    }
    
    threads.join_all();
}
