//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-09-08
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
#include <warp/options.h>
#include <warp/timer.h>
#include <warp/file.h>
#include <warp/strutil.h>
#include <ex/exception.h>
#include <vector>
#include <string>
#include <utility>
#include <iostream>
#include <boost/format.hpp>

using namespace kdi;
using namespace warp;
using namespace ex;
using namespace std;
using boost::format;

namespace
{
    char const * const ALNUM_SET =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789";
}


//----------------------------------------------------------------------------
// dumbRand
//----------------------------------------------------------------------------
inline size_t dumbRand(size_t & x)
{
    return rand_r((unsigned int *)&x);
    //return x = (x << 17) + (x >> 5) + 13;
}


//----------------------------------------------------------------------------
// StringGenerator
//----------------------------------------------------------------------------
class StringGenerator
{
    size_t minLen;
    size_t maxLen;
    vector<char> charSet;

public:
    StringGenerator(size_t minLen, size_t maxLen, string const & charSet) :
        minLen(minLen), maxLen(maxLen),
        charSet(charSet.begin(), charSet.end())
    {
        if(this->charSet.empty())
            raise<ValueError>("empty char set");
    }

    void generate(size_t seed, string & x) const
    {
        size_t len = minLen;
        if(maxLen > minLen)
            len += dumbRand(seed) % (maxLen - minLen);

        x.resize(len);

        size_t sz = charSet.size();
        for(size_t i = 0; i < len; ++i)
            x[i] = charSet[dumbRand(seed) % sz];
    }
};


//----------------------------------------------------------------------------
// CellGenerator
//----------------------------------------------------------------------------
class CellGenerator
{
    StringGenerator rowGen;
    StringGenerator colGen;
    StringGenerator valGen;
    string family;

public:
    CellGenerator(pair<size_t, size_t> const & rowLen,
                  pair<size_t, size_t> const & colLen,
                  pair<size_t, size_t> const & valLen,
                  string const & family) :
        rowGen(rowLen.first, rowLen.second, ALNUM_SET),
        colGen(colLen.first, colLen.second, ALNUM_SET),
        valGen(valLen.first, valLen.second, ALNUM_SET),
        family(family)
    {
    }

    void generate(size_t seed, Cell & x) const
    {
        string r,c,v;

        rowGen.generate(dumbRand(seed), r);
        colGen.generate(dumbRand(seed), c);
        valGen.generate(dumbRand(seed), v);

        x = makeCell(r,family+c,0,v);
    }
};


//----------------------------------------------------------------------------
// parseRange
//----------------------------------------------------------------------------
inline pair<size_t, size_t> parseRange(string const & s)
{
    string::size_type pos = s.find(',');
    size_t min;
    size_t max;
    if(pos == string::npos)
        min = max = parseSize(s);
    else
    {
        min = parseSize(s.substr(0, pos));
        max = parseSize(s.substr(pos + 1));
    }
    return make_pair(min,max);
}

//----------------------------------------------------------------------------
// seedRand
//----------------------------------------------------------------------------
void seedRand()
{
    FilePtr fp = File::input("/dev/random");
    unsigned int seed = 0;
    fp->read(&seed, sizeof(seed));
    fp->close();
    srand(seed);
}

//----------------------------------------------------------------------------
// cellSize
//----------------------------------------------------------------------------
size_t cellSize(Cell const & x)
{
    return x.getRow().size() + x.getColumn().size() +
        x.getValue().size() + 8;
}

void report(size_t nCells, size_t nBytes, double elapsedTime)
{
    cerr << format("loaded %s cells (%s bytes): %s cells/s (%s bytes/s)\n")
        % sizeString(nCells) % sizeString(nBytes)
        % sizeString(size_t(nCells/elapsedTime))
        % sizeString(size_t(nBytes/elapsedTime));
}

//----------------------------------------------------------------------------
// main
//----------------------------------------------------------------------------
int main(int ac, char ** av)
{
    OptionParser op("%prog [options] <table>");
    {
        using namespace boost::program_options;
        op.addOption("rowLen,r", value<string>()->default_value("10"),
                     "Length or range of lengths for generated row keys");
        op.addOption("columnLen,c", value<string>()->default_value("10"),
                     "Length or range of lengths for generated column keys");
        op.addOption("valueLen,v", value<string>()->default_value("10"),
                     "Length or range of lengths for generated values");
        op.addOption("family,f", value<string>(),
                     "Generate cells in this column family");
        op.addOption("verbose,V", "Be verbose");
        op.addOption("number,n", value<string>()->default_value("1"),
                     "Number of cells to load");
        op.addOption("seed,s", value<unsigned int>(),
                     "Random seed");
    }
    
    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    bool verbose = hasopt(opt, "verbose");

    string arg;
    opt.get("rowLen", arg);
    pair<size_t,size_t> rowLen = parseRange(arg);
    opt.get("columnLen", arg);
    pair<size_t,size_t> colLen = parseRange(arg);
    opt.get("valueLen", arg);
    pair<size_t,size_t> valLen = parseRange(arg);
    opt.get("number", arg);
    size_t number = parseSize(arg);

    string family;
    if(opt.get("family", family))
        family += ":";

    unsigned int seed;
    if(opt.get("seed", seed))
        srand(seed);
    else
        seedRand();

    for(ArgumentList::const_iterator ai = args.begin(); ai != args.end(); ++ai)
    {
        if(verbose)
            cerr << "loading table: " << *ai << endl;

        TablePtr table = Table::open(*ai);


        CellGenerator gen(rowLen, colLen, valLen, family);
        Cell x;

        size_t nCells = 0;
        size_t nBytes = 0;
        WallTimer timer;
        while(nCells < number)
        {
            gen.generate(rand(), x);
            table->insert(x);

            ++nCells;
            nBytes += cellSize(x);

            if(verbose && !(nCells%10000))
                report(nCells, nBytes, timer.getElapsed());
        }

        table->sync();
        report(nCells, nBytes, timer.getElapsed());
    }
}
