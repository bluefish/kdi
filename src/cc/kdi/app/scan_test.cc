//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-27
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

#include <kdi/app/scan_visitor.h>
#include <kdi/table.h>
#include <kdi/scan_predicate.h>
#include <warp/options.h>
#include <warp/bloom_filter.h>

#include <boost/scoped_ptr.hpp>
#include <vector>
#include <string>
#include <stdlib.h>

using namespace kdi::app;
using namespace kdi;
using namespace warp;
using namespace ex;
using namespace std;

//----------------------------------------------------------------------------
// Sampler
//----------------------------------------------------------------------------
template <class T>
class Sampler
{
    size_t const sampleSize;
    size_t populationSize;
    std::vector<T> samples;

public:
    explicit Sampler(size_t sampleSize) :
        sampleSize(sampleSize),
        populationSize(0)
    {
        samples.reserve(sampleSize);
    }

    void insert(T const & x)
    {
        ++populationSize;
        if(samples.size() < sampleSize)
        {
            samples.push_back(x);
        }
        else if(double(rand()) * populationSize <
                double(RAND_MAX) * sampleSize)
        {
            size_t idx = size_t(rand() / (1.0 + RAND_MAX));
            samples[idx] = x;
        }
    }

    std::vector<T> const & getSamples() const
    {
        return samples;
    }
};

//----------------------------------------------------------------------------
// UniqueStringSampler
//----------------------------------------------------------------------------
class UniqueStringSampler
{
    Sampler<std::string> sampler;
    BloomFilter filter;
    
public:
    UniqueStringSampler(size_t sampleSize, size_t expectedUniqueCount) :
        sampler(sampleSize),
        filter(12 * expectedUniqueCount, 3)
    {
    }

    void insert(strref_t x)
    {
        if(filter.contains(x))
            return;
        filter.insert(x);
        sampler.insert(x.toString());
    }

    void insert(std::string const & x)
    {
        if(filter.contains(x))
            return;
        filter.insert(x);
        sampler.insert(x);
    }

    std::vector<std::string> const & getSamples() const
    {
        return sampler.getSamples();
    }
};

//----------------------------------------------------------------------------
// SamplingVisitor
//----------------------------------------------------------------------------
class SamplingVisitor
{
    std::string lastRow;
    Sampler<std::string> * rowSampler;
    UniqueStringSampler * columnSampler;
    UniqueStringSampler * familySampler;

public:
    SamplingVisitor(Sampler<std::string> * rowSampler,
                    UniqueStringSampler * columnSampler,
                    UniqueStringSampler * familySampler) :
        rowSampler(rowSampler),
        columnSampler(columnSampler),
        familySampler(familySampler) {}

    void startScan() {}
    void endScan() {}

    void startTable(std::string const & uri) {}
    void endTable() {}

    void visitCell(Cell const & x)
    {
        if(rowSampler && x.getRow() != lastRow)
        {
            lastRow = x.getRow().toString();
            rowSampler->insert(lastRow);
        }
        
        if(columnSampler)
            columnSampler->insert(x.getColumn());
        
        if(familySampler)
            familySampler->insert(x.getColumnFamily());
    }
};

//----------------------------------------------------------------------------
// main
//----------------------------------------------------------------------------
int main(int ac, char ** av)
{
    OptionParser op;
    {
        using namespace boost::program_options;
        op.addOption("predicate,p",    value<string>(), "Use scan predicate");
        op.addOption("rowSample,r",    value<string>(), "Sample K rows");
        op.addOption("columnSample,c", value<string>(), "Sample K columns");
        op.addOption("columnSpace,C",  value<string>()->default_value("50k"),
                     "Expected size of column space");
        op.addOption("familySample,f", value<string>(), "Sample K column families");
        op.addOption("familySpace,F",  value<string>()->default_value("50"),
                     "Expected size of column family space");
        op.addOption("verbose,v", "Be verbose");
    }

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);
    
    bool verbose = hasopt(opt, "verbose");

    string arg;
    
    ScanPredicate pred;
    if(opt.get("predicate", arg))
    {
        try {
            pred = ScanPredicate(arg);
        }
        catch(ValueError const & ex) {
            op.error(ex.what());
        }
        if(verbose)
            cerr << "using predicate: " << pred << endl;
    }
    
    boost::scoped_ptr< Sampler<std::string> > rowSampler;
    boost::scoped_ptr<UniqueStringSampler> columnSampler;
    boost::scoped_ptr<UniqueStringSampler> familySampler;

    if(opt.get("rowSample", arg))
        rowSampler.reset(new Sampler<std::string>(parseSize(arg)));

    if(opt.get("columnSample", arg))
    {
        size_t sz = parseSize(arg);
        opt.get("columnSpace", arg);
        columnSampler.reset(new UniqueStringSampler(sz, parseSize(arg)));
    }

    if(opt.get("familySample", arg))
    {
        size_t sz = parseSize(arg);
        opt.get("familySpace", arg);
        familySampler.reset(new UniqueStringSampler(sz, parseSize(arg)));
    }

    if(rowSampler || columnSampler || familySampler)
    {
        if(verbose)
        {
            cerr << "Sampling..." << endl;

            VerboseAdapter<SamplingVisitor> v(
                SamplingVisitor(
                    rowSampler.get(),
                    columnSampler.get(),
                    familySampler.get()));
            doScan(v, args, pred);

            cerr << "Done Sampling..." << endl;

            if(rowSampler)
                cerr << "Got " << rowSampler->getSamples().size() << " rows" << endl;

            if(columnSampler)
                cerr << "Got " << columnSampler->getSamples().size() << " columns" << endl;

            if(familySampler)
                cerr << "Got " << familySampler->getSamples().size() << " families" << endl;
        }
        else
        {
            SamplingVisitor v(
                rowSampler.get(),
                columnSampler.get(),
                familySampler.get());
            
            doScan(v, args, pred);
        }
    }

    return 0;
}
