//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-10
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

#include <kdi/net/TableManagerI.h>
#include <kdi/net/ScannerLocator.h>
#include <kdi/cell_filter.h>
#include <kdi/marshal/cell_block.h>
#include <kdi/marshal/cell_block_builder.h>
#include <warp/StatTracker.h>
#include <warp/builder.h>
#include <warp/log.h>
#include <warp/fs.h>
#include <ex/exception.h>

#include <boost/algorithm/string.hpp>
#include <assert.h>

#include <Ice/ObjectAdapter.h>

using namespace kdi::net::details;
using namespace std;
using namespace ex;
using namespace warp;
using namespace boost::algorithm;
using Ice::ByteSeq;

namespace
{
    inline void assign(ByteSeq & seq, strref_t str)
    {
        seq.resize(str.size());
        if(!str.empty())
            memcpy(&seq[0], str.begin(), str.size());
    }

    enum {
        BLOCK_THRESHOLD = 100 << 10,      // 100 KB
        SCAN_THRESHOLD  =   2 << 20       //   2 MB
    };

    size_t getScannerId()
    {
        static boost::mutex mutex;
        boost::mutex::scoped_lock lock(mutex);

        static size_t nextId = 0;
        return nextId++;
    }
}


//----------------------------------------------------------------------------
// ScannerI
//----------------------------------------------------------------------------
ScannerI::ScannerI(kdi::TablePtr const & table,
                   kdi::ScanPredicate const & pred,
                   kdi::net::ScannerLocator * locator,
                   warp::StatTracker * tracker) :
    limit(new LimitedScanner(SCAN_THRESHOLD)),
    cellBuilder(&builder),
    locator(locator),
    tracker(tracker)
{
    //log("ScannerI %p: created", this);

    ScanPredicate basePred;
    ScanPredicate filterPred;

    if(pred.getRowPredicate())
        basePred.setRowPredicate(*pred.getRowPredicate());

    if(getenv("UNLIMIT_NET_SCANNERS")) {
        log("Using unlimited net scanner");

        if(pred.getColumnPredicate()) {
            // Only pass down column predicates if they have 
            // column family restrictions that can be used for 
            // fast filtering
            vector<StringRange> fams;
            if(pred.getColumnFamilies(fams)) {
                basePred.setColumnPredicate(*pred.getColumnPredicate());
            } else {
                filterPred.setColumnPredicate(*pred.getColumnPredicate());
            }
        }
        
        // But always pass through the time predicate
        if(pred.getTimePredicate())
            basePred.setTimePredicate(*pred.getTimePredicate());
    } else {
        log("Using limited net scanner");
        if(pred.getColumnPredicate())
            filterPred.setColumnPredicate(*pred.getColumnPredicate());
        if(pred.getTimePredicate())
            filterPred.setTimePredicate(*pred.getTimePredicate());
    }
    if(pred.getMaxHistory())
        filterPred.setMaxHistory(pred.getMaxHistory());

    limit->pipeFrom(table->scan(basePred));
    scan = applyPredicateFilter(filterPred, limit);

    tracker->add("Scanner.nActive", 1);
    tracker->add("Scanner.nOpened", 1);
}

ScannerI::~ScannerI()
{
    //log("ScannerI %p: destroyed", this);

    tracker->add("Scanner.nActive", -1);
}

void ScannerI::getBulk(Ice::ByteSeq & cells, bool & lastBlock,
                       Ice::Current const & cur)
{
    boost::mutex::scoped_lock lock(mutex);

    builder.reset();
    cellBuilder.reset();

    if(limit->fetch())
    {
        Cell x;
        while(scan->get(x))
        {
            cellBuilder.append(x);

            // Is output full?
            if(cellBuilder.getDataSize() >= BLOCK_THRESHOLD)
                break;
        }
    }

    lastBlock = limit->endOfStream();

    builder.finalize();
    cells.resize(builder.getFinalSize());
    builder.exportTo(&cells[0]);

    tracker->add("Scanner.nGets", 1);
    tracker->add("Scanner.getSz", cells.size());
}

void ScannerI::close(Ice::Current const & cur)
{
    boost::mutex::scoped_lock lock(mutex);

    size_t id = locator->getIdFromName(cur.id.name);

    log("Scan: close scanner %d", id);
    locator->remove(id);

    tracker->add("Scanner.nClosed", 1);
}


//----------------------------------------------------------------------------
// TableI
//----------------------------------------------------------------------------
TableI::TableI(kdi::TablePtr const & table,
               std::string const & tablePath,
               kdi::net::ScannerLocator * locator,
               warp::StatTracker * tracker) :
    table(table),
    tablePath(tablePath),
    locator(locator),
    tracker(tracker)
{
    //log("TableI %p: created", this);
    assert(table);
    assert(locator);

    tracker->add("Table.nActive", 1);
}

TableI::~TableI()
{
    // try {
    //     table->sync();
    // }
    // catch(std::exception const & ex) {
    //     log("TableI %p: final sync() failed: %s", this, ex.what());
    // }
    // catch(...) {
    //     log("TableI %p: final sync() failed", this);
    // }

    //log("TableI %p: destroyed", this);

    tracker->add("Table.nActive", -1);
}

void TableI::applyMutations(Ice::ByteSeq const & cells,
                            Ice::Current const & cur)
{
    using kdi::marshal::CellBlock;
    using kdi::marshal::CellData;

    assert(!cells.empty());

    CellBlock const * b = reinterpret_cast<CellBlock const *>(&cells[0]);

    size_t nSet = 0;
    size_t nErase = 0;
    
    for(CellData const * ci = b->cells.begin(); ci != b->cells.end(); ++ci)
    {
        if(ci->value)
        {
            ++nSet;
            table->set(*ci->key.row, *ci->key.column,
                       ci->key.timestamp, *ci->value);
        }
        else
        {
            ++nErase;
            table->erase(*ci->key.row, *ci->key.column,
                         ci->key.timestamp);
        }
    }

    tracker->add("Table.nApply", 1);
    tracker->add("Table.applySz", cells.size());
    tracker->add("Table.nSet", nSet);
    tracker->add("Table.nErase", nErase);
}

void TableI::sync(Ice::Current const & cur)
{
    table->sync();
    tracker->add("Table.nSync", 1);
}

ScannerPrx TableI::scan(std::string const & predicate,
                        Ice::Current const & cur)
{
    // Parse predicate
    ScanPredicate pred(predicate);

    // Get unique scanner ID
    size_t scannerId = getScannerId();

    // Make identity
    Ice::Identity id;
    id.category = "scan";
    id.name = locator->getNameFromId(scannerId);

    // Make ICE object for scanner
    ScannerIPtr obj = new ScannerI(table, pred, locator, tracker);
    size_t nActive = locator->add(scannerId, obj);

    // Report
    log("Scan: open scanner %d on %s pred=(%s), %d active",
        scannerId, tablePath, pred, nActive);

    return ScannerPrx::uncheckedCast(cur.adapter->createProxy(id));
}


//----------------------------------------------------------------------------
// TableManagerI
//----------------------------------------------------------------------------
TableManagerI::TableManagerI(warp::StatTracker * tracker) :
    tracker(tracker)
{
    //log("TableManagerI %p: created", this);
}

TableManagerI::~TableManagerI()
{
    //log("TableManagerI %p: destroyed", this);
}

TablePrx TableManagerI::openTable(std::string const & path,
                                  Ice::Current const & cur)
{
    // We want to interpret all paths as relative to the server's root
    // directory.  Here we normalize the path and make it relative.
    // The resulting path is later resolved against the server root
    // when the table is created.  To normalize the path, we first
    // strip out all non-path characters and then resolve the path
    // against "/" to get rid of any back references.  The resulting
    // path mush be absolute, so we just trim the leading slash to
    // make it relative.
    string tablePath = fs::resolve("/", fs::path(path)).substr(1);

    Ice::Identity id;
    id.category = "table";
    id.name = tablePath;

    // Strip trailing slashes
    trim_right_if(id.name, is_any_of("/"));

    //log("TableManagerI::openTable(%s)", id.name);

    tracker->add("TableManager.open", 1);

    // Return proxy
    return TablePrx::uncheckedCast(cur.adapter->createProxy(id));
}
