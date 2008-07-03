//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-10
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

#include <kdi/net/TableManagerI.h>
#include <kdi/marshal/cell_block.h>
#include <kdi/marshal/cell_block_builder.h>
#include <warp/builder.h>
#include <warp/log.h>
#include <warp/fs.h>
#include <ex/exception.h>

#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/format.hpp>
#include <Ice/ObjectAdapter.h>
#include <Ice/ServantLocator.h>
#include <IceUtil/UUID.h>
#include <iostream>
#include <map>

using namespace kdi::net::details;
using namespace std;
using namespace ex;
using namespace warp;
using boost::format;
using namespace boost::algorithm;
using Ice::ByteSeq;

namespace
{
    inline void assign(ByteSeq & seq, str_data_t const & str)
    {
        seq.resize(str.size());
        if(!str.empty())
            memcpy(&seq[0], str.begin(), str.size());
    }
}


//----------------------------------------------------------------------------
// ScannerI
//----------------------------------------------------------------------------
ScannerI::ScannerI(kdi::CellStreamPtr const & scan,
                   TimeoutLocatorPtr const & locator) :
    scan(scan), cellBuilder(&builder),
    lastAccess(kdi::Timestamp::now()),
    locator(locator)
{
    //log("ScannerI %p: created", this);

    assert(scan);
    assert(locator);
}

ScannerI::~ScannerI()
{
    //log("ScannerI %p: destroyed", this);
}

void ScannerI::getBulk(Ice::ByteSeq & cells, bool & lastBlock,
                       Ice::Current const & cur)
{
    enum { BLOCK_THRESHOLD = 100 << 10 }; // 100k

    lastAccess = kdi::Timestamp::now();

    lastBlock = false;
    builder.reset();
    cellBuilder.reset();

    Cell x;
    for(;;)
    {
        if(!scan->get(x))
        {
            lastBlock = true;
            break;
        }

        cellBuilder.append(x);
        if(cellBuilder.getDataSize() >= BLOCK_THRESHOLD)
            break;
    }

    builder.finalize();
    cells.resize(builder.getFinalSize());
    builder.exportTo(&cells[0]);

    // cerr << format("getBulk: %d cells, %d bytes, eof=%s")
    //     % cellBuilder.getCellCount() % cells.size() % lastBlock
    //      << endl;
}

void ScannerI::close(Ice::Current const & cur)
{
    locator->remove(cur.id);
}


//----------------------------------------------------------------------------
// TableI
//----------------------------------------------------------------------------
TableI::TableI(kdi::TablePtr const & table,
               TimeoutLocatorPtr const & locator) :
    table(table),
    lastAccess(kdi::Timestamp::now()),
    locator(locator)
{
    //log("TableI %p: created", this);

    assert(table);
    assert(locator);
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
}

void TableI::applyMutations(Ice::ByteSeq const & cells,
                            Ice::Current const & cur)
{
    using kdi::marshal::CellBlock;
    using kdi::marshal::CellData;
    
    lastAccess = kdi::Timestamp::now();

    assert(!cells.empty());

    CellBlock const * b = reinterpret_cast<CellBlock const *>(&cells[0]);

    // cerr << format("Apply buffer size: %d cells, %d bytes")
    //     % b->cells.size() % cells.size()
    //      << endl;

    for(CellData const * ci = b->cells.begin(); ci != b->cells.end(); ++ci)
    {
        if(ci->value)
        {
            table->set(ci->key.row, ci->key.column,
                       ci->key.timestamp, ci->value);
        }
        else
        {
            table->erase(ci->key.row, ci->key.column,
                         ci->key.timestamp);
        }
    }
}

void TableI::sync(Ice::Current const & cur)
{
    lastAccess = kdi::Timestamp::now();

    table->sync();
}

ScannerPrx TableI::scan(std::string const & predicate,
                        Ice::Current const & cur)
{
    lastAccess = kdi::Timestamp::now();

    // Open scan
    kdi::CellStreamPtr scan = table->scan(predicate);

    // Make identity
    Ice::Identity id;
    id.category = "scan";
    id.name = IceUtil::generateUUID();

    // Make ICE object for scanner
    ScannerIPtr obj = new ScannerI(scan, locator);
    locator->add(id, obj, 7200);

    return ScannerPrx::uncheckedCast(cur.adapter->createProxy(id));
}


//----------------------------------------------------------------------------
// TableManagerI
//----------------------------------------------------------------------------
TableManagerI::TableManagerI(TimeoutLocatorPtr const & locator) :
    locator(locator)
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

    // Return proxy
    return TablePrx::uncheckedCast(cur.adapter->createProxy(id));
}
