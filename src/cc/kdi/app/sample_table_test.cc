//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-19
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
#include <kdi/row_stream.h>
#include <kdi/scan_predicate.h>
#include <warp/options.h>
#include <warp/log.h>
#include <string>

using namespace kdi;
using namespace warp;
using namespace std;

int getRandThreshold(double passRate)
{
    int pass = int(RAND_MAX * passRate);
    if(pass > RAND_MAX)
        pass = RAND_MAX;
    return pass;
}

void sampleStream(CellStreamPtr const & input,
                  TablePtr const & output,
                  double cellRate)
{
    if(cellRate < 1)
    {
        int pass = getRandThreshold(cellRate);
        Cell x;
        while(input->get(x))
        {
            if(rand() <= pass)
                output->insert(x);
        }
    }
    else
    {
        Cell x;
        while(input->get(x))
            output->insert(x);
    }
}

void sampleTable(TablePtr const & input,
                 TablePtr const & output,
                 double cellRate,
                 double rowRate,
                 double intervalRate)
{
    if(intervalRate <= 0 || rowRate <= 0 || cellRate <= 0)
        return;

    ScanPredicate pred;

    if(intervalRate < 1)
    {
        log("Filtering row intervals (pass rate = %g)", intervalRate);

        IntervalSet<string> rows;
        int pass = getRandThreshold(intervalRate);

        RowIntervalStreamPtr intervals = input->scanIntervals();
        RowInterval x;
        while(intervals->get(x))
        {
            if(rand() <= pass)
            {
                log("Using interval: %s", x);
                rows.add(x);
            }
        }

        pred.setRowPredicate(rows);
    }

    if(rowRate < 1)
    {
        log("Filtering rows (pass rate = %g)", rowRate);

        int pass = getRandThreshold(rowRate);

        CellStreamPtr rows(new RowStream(input->scan(pred)));
        while(rows->fetch())
        {
            if(rand() <= pass)
                sampleStream(rows, output, cellRate);
        }
    }
    else
    {
        sampleStream(input->scan(pred), output, cellRate);
    }

    output->sync();
}

int main(int ac, char ** av)
{
    OptionParser op;
    {
        using namespace boost::program_options;
        op.addOption("output,o", value<string>(), "Output table");
        op.addOption("cellRate,c", value<double>()->default_value(1.0),
                     "Cell sample rate");
        op.addOption("rowRate,r", value<double>()->default_value(1.0),
                     "Row sample rate");
        op.addOption("intervalRate,i", value<double>()->default_value(1.0),
                     "Table interval sample rate");
    }

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    double cellRate = 1.0;
    opt.get("cellRate", cellRate);

    double rowRate = 1.0;
    opt.get("rowRate", rowRate);

    double intervalRate = 1.0;
    opt.get("intervalRate", intervalRate);

    string arg;
    if(!opt.get("output", arg))
        op.error("need --output");

    TablePtr output = Table::open(arg);

    for(ArgumentList::const_iterator ai = args.begin();
        ai != args.end(); ++ai)
    {
        log("Sampling %s", *ai);

        sampleTable(Table::open(*ai), output, cellRate,
                    rowRate, intervalRate);
    }
}
