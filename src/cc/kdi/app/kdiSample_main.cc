//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-27
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
#include <kdi/scan_predicate.h>
#include <kdi/RowInterval.h>
#include <warp/options.h>
#include <warp/bloom_filter.h>
#include <boost/format.hpp>
#include <iostream>
#include <vector>
#include <string>
#include <tr1/unordered_map>

using namespace kdi;
using namespace warp;
using namespace std;
using boost::format;

enum {
    N_ROWS = 0,
    N_COLS,
    N_CELLS,
        
    ROW_SZ,
    COL_SZ,
    VAL_SZ,

    NUM_STATS
};

//----------------------------------------------------------------------------
// StatCollector
//----------------------------------------------------------------------------
class StatCollector
{
    typedef std::tr1::unordered_map<std::string, size_t> smap_t;

    size_t nTotal;
    size_t nScanned;

    std::vector<size_t> next;
    std::vector<size_t> total;
    std::vector<size_t> dTotal;

    smap_t familyMap;
    BloomFilter columnFilter;
    std::string lastRow;
    std::string family;

    static void assign(strref_t src, std::string & dst)
    {
        dst.assign(src.begin(), src.end());
    }

public:
    StatCollector(size_t nTotal) :
        nTotal(nTotal),
        nScanned(0),
        next(NUM_STATS, 0),
        total(NUM_STATS, 0),
        dTotal(NUM_STATS, 0),
        columnFilter(64 << 20, 3)
    {
    }

    void sample(Cell const & x)
    {
        ++next[N_CELLS];

        strref_t r  = x.getRow();
        strref_t c  = x.getColumn();
        strref_t cf = x.getColumnFamily();
        strref_t v  = x.getValue();

        next[ROW_SZ] += r.size();
        next[COL_SZ] += c.size();
        next[VAL_SZ] += v.size();
        
        if(r != lastRow)
        {
            ++next[N_ROWS];
            assign(r, lastRow);
        }

        assign(cf, family);
        ++familyMap[family];
        
        if(!columnFilter.contains(c))
        {
            ++next[N_COLS];
            columnFilter.insert(c);
        }
    }

    void endInterval()
    {
        for(size_t i = 0; i < NUM_STATS; ++i)
        {
            //cout << format("i:%d d:%d t:%d n:%d") % i % dTotal[i] % total[i] % next[i]
            //     << endl;

            size_t a = std::min(size_t(10), nScanned);

            dTotal[i] = (dTotal[i]*a + next[i]) / (a+1);
            total[i] += next[i];
            next[i] = 0;

            //cout << format("S:%d d:%d t:%d n:%d") % nScanned % dTotal[i] % total[i] % next[i]
            //     << endl;
        }
        ++nScanned;
    }
    
    size_t getCurrent(size_t i) const
    {
        return total[i] + next[i];
    }

    size_t getEstimate(size_t i) const
    {
        size_t r = total[i];
        if(nScanned < nTotal)
            r += dTotal[i] * (nTotal - nScanned);
        return r;
    }
};


//----------------------------------------------------------------------------
// StatReporter
//----------------------------------------------------------------------------
class StatReporter
{
public:
    StatReporter(StatCollector const & c) : c(c) {}

    void emit(std::ostream & out) const
    {
        out << format("%s/%s rows, %s/%s cols, %s/%s cells")
            % cur(N_ROWS) % est(N_ROWS)
            % cur(N_COLS) % est(N_COLS)
            % cur(N_CELLS) % est(N_CELLS)
            << endl;
        
        out << format("%s/%s rowSz, %s/%s colSz, %s/%s valSz")
            % cur(ROW_SZ) % est(ROW_SZ)
            % cur(COL_SZ) % est(COL_SZ)
            % cur(VAL_SZ) % est(VAL_SZ)
            << endl;
    }

private:
    StatCollector const & c;

    inline string cur(size_t i) const
    {
        return sizeString(c.getCurrent(i));
    }

    inline string est(size_t i) const
    {
        return sizeString(c.getEstimate(i));
    }
};

std::ostream & operator<<(std::ostream & out, StatReporter const & x)
{
    x.emit(out);
    return out;
}


//----------------------------------------------------------------------------
// main
//----------------------------------------------------------------------------
int main(int ac, char ** av)
{
    OptionParser op("%prog [options] <table> ...");
    {
        using namespace boost::program_options;
    }

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    for(ArgumentList::const_iterator ai = args.begin();
        ai != args.end(); ++ai)
    {
        cout << "Scanning " << *ai << endl;

        TablePtr table = Table::open(*ai);
        vector<RowInterval> intervals;
        {
            RowIntervalStreamPtr scan = table->scanIntervals();
            RowInterval x;
            while(scan->get(x))
                intervals.push_back(x);
        }

        size_t nIntervals = intervals.size();
        cout << format("  got %d intervals") % nIntervals << endl;

        StatCollector collector(intervals.size());
        while(!intervals.empty())
        {
            size_t idx = rand() % intervals.size();
            RowInterval & ri = intervals[idx];

            cout << "Sampling: " << ri << endl;
            CellStreamPtr scan = table->scan(ri.toRowPredicate());

            ri = intervals.back();
            intervals.pop_back();

            Cell x;
            while(scan->get(x))
                collector.sample(x);
            collector.endInterval();

            cout << format("Completed.  %d/%d remaining")
                % intervals.size() % nIntervals
                 << endl
                 << StatReporter(collector)
                 << endl;
        }
    }

    return 0;
}
