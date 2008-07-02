//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/memory_table.cc#1 $
//
// Created 2007/09/19
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/memory_table.h>
#include <kdi/cell_filter.h>
#include <ex/exception.h>
#include <vector>

using namespace kdi;
using namespace warp;
using namespace ex;
using namespace std;

//----------------------------------------------------------------------------
// Constants
//----------------------------------------------------------------------------
namespace {

    /// Approximate fixed overhead per cell in table
    size_t const CELL_OVERHEAD = (
        sizeof(string)*3 +      // variable length data
        sizeof(uint64_t)*2 +    // timestamp and ref count
        sizeof(set<Cell>::value_type) // set value overhead
        );

}

//----------------------------------------------------------------------------
// MemoryTable::Scanner
//----------------------------------------------------------------------------
class MemoryTable::Scanner : public CellStream
{
    boost::shared_ptr<MemoryTable const> table;
    MemoryTable::set_t::const_iterator tableIt;

public:
    Scanner(boost::shared_ptr<MemoryTable const> const & table,
            MemoryTable::set_t::const_iterator const & tableIt) :
        table(table),
        tableIt(tableIt)
    {
        EX_CHECK_NULL(table);
    }

    bool get(Cell & x)
    {
        if(tableIt == table->cells.end())
            return false;

        x = tableIt->cell;
        ++tableIt;
        return true;
    }
};

//----------------------------------------------------------------------------
// MemoryTable
//----------------------------------------------------------------------------
void MemoryTable::insert(Cell const & cell)
{
    // Try to insert the cell -- if it is already in the 
    pair<set_t::iterator, bool> r = cells.insert(Item(cell));

    // If it was already in the set, replace the Cell
    if(!r.second)
    {
        // Update memory usage for the change in value of the
        // overwritten Cell
        memUsage -= r.first->cell.getValue().size();
        memUsage += cell.getValue().size();

        // Replace the Cell
        r.first->cell = cell;
    }
    else
    {
        // Update memory usage (approximately).  We assume a constant
        // overhead per Cell in the table, plus the size of the string
        // data.
        memUsage += (CELL_OVERHEAD +
                     cell.getRow().size() +
                     cell.getColumn().size() +
                     cell.getValue().size());
    }
}

MemoryTable::MemoryTable(bool filterErasures) :
    memUsage(0), filterErasures(filterErasures)
{
    // cerr << "create: MemoryTable " << (void*)this << endl;
}

MemoryTable::~MemoryTable()
{
    // cerr << "delete: MemoryTable " << (void*)this 
    //      << " sz=" << sizeString(memUsage) << endl;
}

MemoryTablePtr MemoryTable::create(bool filterErasures)
{
    MemoryTablePtr ptr(new MemoryTable(filterErasures));
    return ptr;
}

void MemoryTable::set(strref_t row, strref_t column, int64_t timestamp,
                      strref_t value)
{
    // Insert a cell, possibly overwriting an existing cell
    insert(makeCell(row, column, timestamp, value));
}

void MemoryTable::erase(strref_t row, strref_t column, int64_t timestamp)
{
    // Insert a cell erasure, possibly overwriting an existing cell
    insert(makeCellErasure(row, column, timestamp));
}

CellStreamPtr MemoryTable::scan() const
{
    if(filterErasures)
    {
        CellStreamPtr s = makeErasureFilter();
        s->pipeFrom(scanWithErasures());
        return s;
    }
    else
        return scanWithErasures();
}

CellStreamPtr MemoryTable::scan(ScanPredicate const & pred) const
{
    if(!filterErasures && pred.getMaxHistory())
        raise<ValueError>("cannot have history predicate with erasures");

    return applyPredicateFilter(pred, scan());
}

size_t MemoryTable::getMemoryUsage() const
{
    return memUsage;
}

size_t MemoryTable::getCellCount() const
{
    return cells.size();
}

CellStreamPtr MemoryTable::scanWithErasures() const
{
    // Create a new Scanner starting at the beginning of the table
    CellStreamPtr s(
        new Scanner(
            shared_from_this(),
            cells.begin()
            )
        );
    return s;
}


//----------------------------------------------------------------------------
// Registration
//----------------------------------------------------------------------------
#include <warp/init.h>
#include <warp/uri.h>
#include <kdi/table_factory.h>
#include <map>

namespace
{
    TablePtr myOpen(string const & uri)
    {
        // The contents of named memory tables (those with a non-empty
        // path) persist for the life of the process.

        // See if this is a named table
        string name = uriDecode(Uri(wrap(uri)).path);
        if(!name.empty())
        {
            // Named table

            // The cache is a static map of MemoryTables.
            // MemoryTables contain dynamic cells and erasures.
            // However, if someone asks for a named table before
            // creating any cells (not too unlikely), the static cache
            // will be initialized before the static dynamic cell
            // interpreters.  This is fine, except when it's time for
            // the program to exit.  The interpreters will be
            // destructed before the table cache can be cleared: boom.
            // To make sure the cell interpreters get initialized
            // first, we make a couple of static cells before
            // declaring the cache.
            static Cell uglyHack = makeCell("","",0,"");
            static Cell uglyHack2 = makeCellErasure("","",0);
            static map<string, TablePtr> cache;

            // Get the table out of the cache.  If it is null, create
            // a new one.
            TablePtr & tbl = cache[name];
            if(!tbl)
                tbl = MemoryTable::create(true); 
            return tbl;
        }
        else
        {
            // Temp table
            return MemoryTable::create(true); 
        }
    }
}

WARP_DEFINE_INIT(kdi_memory_table)
{
    TableFactory::get().registerTable("mem", &myOpen);
}
