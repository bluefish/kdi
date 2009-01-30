//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-29
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

#include <warp/options.h>
#include <warp/file.h>
#include <warp/log.h>
#include <warp/buffer.h>
#include <warp/syncqueue.h>
#include <warp/varsub.h>
#include <warp/call_or_die.h>
#include <kdi/table.h>
#include <kdi/scan_predicate.h>
#include <string>
#include <vector>
#include <map>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>

using namespace kdi;
using namespace kdi::app;
using namespace warp;
using namespace std;
using boost::format;

struct ScanJob
{
    std::string table;
    std::string pred;

    ScanJob() {}
    ScanJob(strref_t table, strref_t pred) :
        table(table.begin(), table.end()),
        pred(pred.begin(), pred.end()) {}
};

inline StringRange find_first(char const * begin, char const * end,
                              char const * pattern)
{
    StringRange s(begin, end);
    StringRange p(pattern);
    return boost::algorithm::find_first(s, p);
}

inline StringRange find_last(char const * begin, char const * end,
                             char const * pattern)
{
    StringRange s(begin, end);
    StringRange p(pattern);
    return boost::algorithm::find_last(s, p);
}

typedef warp::SyncQueue<ScanJob> ScanQueue;

void loadThread(vector<string> const & files, ScanQueue * jobQ)
{
    log("Load thread starting");

    Buffer buf(1<<20);

    for(vector<string>::const_iterator i = files.begin();
        i != files.end(); ++i)
    {
        log("Loading: %s", *i);
        size_t nLines = 0;
        size_t nJobs = 0;

        FilePtr fp = File::input(*i);
        while(fp->readline(buf, 128 << 20))
        {
            ++nLines;
            buf.flip();

            StringRange pre = find_first(buf.position(), buf.limit(), " on ");
            if(!pre)
                continue;

            StringRange mid = find_first(pre.end(), buf.limit(), " pred=(");
            if(!mid)
                continue;

            StringRange end = find_last(mid.end(), buf.limit(), "), ");
            if(!end)
                continue;

            StringRange table(pre.end(), mid.begin());
            StringRange pred(mid.end(), end.begin());

            jobQ->push(ScanJob(table, pred));
            ++nJobs;
        }

        log("Loaded %d job(s) from %d line(s): %s", nJobs, nLines, *i);
    }

    jobQ->cancelWaits();

    log("Load thread exiting");
}

void scanThread(size_t scanId, bool verbose,
                std::string const & uriTemplate,
                ScanQueue * jobQ)
{
    log("Scan thread %d starting", scanId);

    AddingVisitor v;
    vector<string> tables(1);
    map<string,string> vmap;

    ScanJob job;
    size_t nScans = 0;
    for(; jobQ->pop(job); ++nScans)
    {
        vmap["TABLE"] = job.table;
        tables[0] = varsub(uriTemplate, vmap);

        ScanPredicate p(job.pred);

        if(verbose)
            log("Scan %d.%d running: table=%s pred=(%s)",
                scanId, nScans, job.table, p);

        size_t nCells = v.nCells;
        size_t nBytes = v.nBytes;

        doScan(v, tables, p);

        if(verbose)
            log("Scan %d.%d done: %s cells, %s bytes", scanId, nScans,
                sizeString(v.nCells - nCells, 1000),
                sizeString(v.nBytes - nBytes, 1024));
    }

    log("Scan thread %d done: %d scans, %s cells, %s bytes",
        scanId, nScans,
        sizeString(v.nCells, 1000),
        sizeString(v.nBytes, 1024));
}

int main(int ac, char ** av)
{
    OptionParser op("%prog [options] log-files...");
    {
        using namespace boost::program_options;
        op.addOption("uriTemplate,u", value<string>()->default_value("kdi:$TABLE"),
                     "Table URI template containing $TABLE");
        op.addOption("numThreads,n", value<size_t>()->default_value(1),
                     "Number of scanner threads");
        op.addOption("verbose,v", "Be verbose");
    }

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    bool verbose = hasopt(opt, "verbose");

    string uriTemplate;
    opt.get("uriTemplate", uriTemplate);

    size_t nThreads = 1;
    opt.get("numThreads", nThreads);
    if(!nThreads)
        op.error("need positive --numThreads");

    ScanQueue jobQ(5000);
    boost::thread_group threads;

    threads.create_thread(
        warp::callOrDie(
            boost::bind(
                &loadThread, boost::cref(args), &jobQ),
            "Loader thread", false));

    for(size_t i = 0; i < nThreads; ++i)
    {
        threads.create_thread(
            warp::callOrDie(
                boost::bind(
                    &scanThread, i, verbose,
                    boost::cref(uriTemplate), &jobQ),
                str(format("Scan thread %d") % i), false));
    }

    threads.join_all();

    return 0;
}
