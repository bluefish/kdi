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
#include <warp/log.h>
#include <warp/string_interval.h>

#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
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
            size_t idx = size_t(rand() * sampleSize / (1.0 + RAND_MAX));
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
// AddingVisitor
//----------------------------------------------------------------------------
class AddingVisitor
{
public:
    size_t nCells;
    size_t nBytes;

    AddingVisitor() : nCells(0), nBytes(0) {}

    void startScan() {}
    void endScan() {}

    void startTable(std::string const & uri) {}
    void endTable() {}

    void visitCell(Cell const & x)
    {
        ++nCells;
        nBytes += (8 + x.getRow().size() + x.getColumn().size() +
                   x.getValue().size());
    }
};


//----------------------------------------------------------------------------
// chooseSample
//----------------------------------------------------------------------------
IntervalSet<std::string>
chooseSample(std::vector<std::string> const & samples, bool family)
{
    IntervalSet<std::string> ss;

    if(!samples.empty())
    {
        size_t idx = size_t(rand() * samples.size() / (1.0 + RAND_MAX));
        std::string const & s = samples[idx];
        if(family)
            ss.add(makePrefixInterval(s + ":"));
        else
            ss.add(Interval<string>().setPoint(s));
    }
    
    return ss;
}

//----------------------------------------------------------------------------
// runScanners
//----------------------------------------------------------------------------
void runScanners(size_t id,
                 size_t nScans,
                 bool verbose,
                 std::vector<std::string> const & tables,
                 ScanPredicate const & pred,
                 Sampler<std::string> * rowSampler,
                 UniqueStringSampler * columnSampler,
                 UniqueStringSampler * familySampler)
{
    for(size_t i = 0; i < nScans; ++i)
    {
        ScanPredicate p(pred);

        if(rowSampler)
        {
            p.setRowPredicate(
                chooseSample(rowSampler->getSamples(), false));
        }

        if(columnSampler)
        {
            if(familySampler && (i & 1))
            {
                p.setColumnPredicate(
                    chooseSample(familySampler->getSamples(), true));
            }
            else
            {
                p.setColumnPredicate(
                    chooseSample(columnSampler->getSamples(), false));
            }
        }
        else if(familySampler)
        {
            p.setColumnPredicate(
                chooseSample(familySampler->getSamples(), true));
        }
        
        log("Scan %d.%d starting: pred=(%s)", id, i, p);

        AddingVisitor v;
        doScan(v, tables, p);

        log("Scan %d.%d done: %s cells, %s bytes", id, i,
            sizeString(v.nCells, 1000),
            sizeString(v.nBytes, 1024));
    }
}


//----------------------------------------------------------------------------
// main
//----------------------------------------------------------------------------
int main(int ac, char ** av)
{
    OptionParser op;
    {
        using namespace boost::program_options;
        op.addOption("predicate,p",    value<string>(), "Use scan predicate");
        op.addOption("numScans,n",     value<string>()->default_value("1"),
                     "Do K scans per scan thread");
        op.addOption("numThreads,N",   value<string>()->default_value("1"),
                     "Use K scanning threads");
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
            log("using predicate: %s", pred);
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
        log("Sampling...");

        if(verbose)
        {
            VerboseAdapter<SamplingVisitor> v(
                SamplingVisitor(
                    rowSampler.get(),
                    columnSampler.get(),
                    familySampler.get()));
            doScan(v, args, pred);
        }
        else
        {
            SamplingVisitor v(
                rowSampler.get(),
                columnSampler.get(),
                familySampler.get());
            
            doScan(v, args, pred);
        }

        log("Done sampling");
    }

    
    if(rowSampler)
        log("Got %d rows", rowSampler->getSamples().size());

    if(columnSampler)
        log("Got %d columns", columnSampler->getSamples().size());
    
    if(familySampler)
        log("Got %d families", familySampler->getSamples().size());

    size_t nScans = 1;
    if(opt.get("numScans", arg))
        nScans = parseSize(arg);

    size_t nThreads = 1;
    if(opt.get("numThreads", arg))
        nThreads = parseSize(arg);

    log("Starting %d thread(s) to do %d scan(s) each", nThreads, nScans);
    boost::thread_group threads;
    for(size_t i = 0; i < nThreads; ++i)
    {
        threads.create_thread(
            boost::bind(&runScanners,
                        i,
                        nScans,
                        verbose,
                        boost::cref(args),
                        boost::cref(pred),
                        rowSampler.get(),
                        columnSampler.get(),
                        familySampler.get()));
    }

    threads.join_all();

    return 0;
}
