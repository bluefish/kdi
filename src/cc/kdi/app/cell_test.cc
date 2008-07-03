//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-02-21
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

#include <kdi/cell.h>
#include <warp/syncqueue.h>
#include <warp/options.h>
#include <boost/thread.hpp>
#include <boost/format.hpp>
#include <boost/bind.hpp>
#include <iostream>
#include <vector>

using namespace kdi;
using namespace warp;
using namespace std;
using boost::format;

namespace
{
    typedef boost::mutex mutex_t;
    typedef mutex_t::scoped_lock lock_t;

    mutex_t printMutex;
}

class CellPump
{
    size_t pumpIdx;
    boost::thread_group producers;
    boost::thread_group consumers;
    SyncQueue<Cell> cellQ;

    static char const * randstr(char * buf, size_t len)
    {
        if(len < 2)
            return "wtf?";
        
        char const ALPHA[] = "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        size_t const N_ALPHA = strlen(ALPHA);

        len = rand() % (len-1);
        char * p = buf;
        *p++ = ALPHA[rand() % N_ALPHA];
        for(size_t i = 0; i < len; ++i)
            *p++ = ALPHA[rand() % N_ALPHA];
        *p = 0;
        return buf;
    }

    void produce(size_t threadIdx, size_t nCells)
    {
        {
            lock_t l(printMutex);
            cout << format("Pump %d, producer %d: starting")
                % pumpIdx % threadIdx << endl;
        }

        char row[64], col[64], val[64];
        for(size_t i = 0; i < nCells; ++i)
        {
            cellQ.push(
                makeCell(
                    randstr(row, sizeof(row)),
                    randstr(col, sizeof(col)),
                    rand(),
                    randstr(val, sizeof(val))
                    )
                );
        }

        {
            lock_t l(printMutex);
            cout << format("Pump %d, producer %d: produced %d cells")
                % pumpIdx % threadIdx % nCells << endl;
        }
    }
    
    void consume(size_t threadIdx)
    {
        {
            lock_t l(printMutex);
            cout << format("Pump %d, consumer %d: starting")
                % pumpIdx % threadIdx << endl;
        }

        size_t nCells = 0;
        size_t nBytes = 0;
        
        Cell x;
        while(cellQ.pop(x))
        {
            ++nCells;
            nBytes += x.getRow().size() + x.getColumn().size() +
                x.getValue().size();
        }

        {
            lock_t l(printMutex);
            cout << format("Pump %d, consumer %d: consumed %d cells, %d bytes")
                % pumpIdx % threadIdx % nCells % nBytes << endl;
        }
    }


public:
    CellPump(size_t pumpIdx, size_t nProducers,
             size_t nConsumers, size_t nCells) :
        pumpIdx(pumpIdx), cellQ(2000)
    {
        for(size_t i = 0; i < nConsumers; ++i)
        {
            consumers.create_thread(
                boost::bind(
                    &CellPump::consume,
                    this, i)
                );
        }

        for(size_t i = 0; i < nProducers; ++i)
        {
            producers.create_thread(
                boost::bind(
                    &CellPump::produce,
                    this, i,
                    nCells / nProducers +
                    ( nCells % nProducers > i ? 1 : 0 )
                    )
                );
        }
    }

    ~CellPump()
    {
        producers.join_all();
        cellQ.cancelWaits();
        consumers.join_all();
    }
};

int main(int ac, char ** av)
{
    OptionParser op;
    {
        using namespace boost::program_options;
        op.addOption("pumps,k", value<size_t>()->default_value(1),
                     "Number of Cell pumps");
        op.addOption("cells,n", value<size_t>()->default_value(10000),
                     "Number of Cells per pump");
        op.addOption("producers,p", value<size_t>()->default_value(1),
                     "Number of producer threads per pump");
        op.addOption("consumers,c", value<size_t>()->default_value(1),
                     "Number of consumer threads per pump");
    }
    
    OptionMap opt;
    ArgumentList arg;
    op.parseOrBail(ac, av, opt, arg);

    size_t nPumps = 1;
    size_t nCells = 10000;
    size_t nProducers = 1;
    size_t nConsumers = 1;

    opt.get("pumps", nPumps);
    opt.get("cells", nCells);
    opt.get("producers", nProducers);
    opt.get("consumers", nConsumers);

    vector<CellPump *> pumps;
    for(size_t i = 0; i < nPumps; ++i)
    {
        pumps.push_back(new CellPump(i, nProducers, nConsumers, nCells));
    }

    for(size_t i = 0; i < nPumps; ++i)
    {
        delete pumps[i];
    }

    return 0;
}
