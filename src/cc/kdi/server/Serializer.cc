//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-11
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

#include <boost/bind.hpp>
#include <kdi/server/Serializer.h>
#include <kdi/server/FragmentMerge.h>
#include <kdi/server/TabletEventListener.h>
#include <kdi/server/DirectBlockCache.h>
#include <kdi/server/TableSchema.h>
#include <warp/call_or_die.h>
#include <warp/fs.h>
#include <warp/log.h>

using namespace kdi;
using namespace kdi::server;
using std::string;
using std::vector;
using std::map;
using std::search;
using warp::Interval;
using warp::IntervalSet;
using warp::log;
using warp::File;

Serializer::Serializer() :
    output(64 << 10),
    quit(false)
{
}

void Serializer::addRow(strref_t row)
{
    if(rows.empty() || row != rows.back())
        rows.push_back(row);
}

void Serializer::operator()(vector<FragmentCPtr> frags, string const & fn,
                            TableSchema::Group const & group)
{
    log("Serializing to %s", fn);
    output.open(fn);
    rows.clear();
    
    DirectBlockCache cache;
    ScanPredicate pred = group.getPredicate();
    
    FragmentMerge merge(frags, &cache, pred, 0);
    const size_t maxCells = 10000;
    const size_t maxSize = 10000;
    while(merge.copyMerged(maxCells, maxSize, *this));

    output.close();
}

void Serializer::emitCell(strref_t row, strref_t column, int64_t timestamp,
                          strref_t value)
{
    addRow(row);
    output.emitCell(row, column, timestamp, value);
}

void Serializer::emitErasure(strref_t row, strref_t column, int64_t timestamp)
{
    addRow(row);
    output.emitErasure(row, column, timestamp);
}

size_t Serializer::getCellCount() const 
{
    return output.getCellCount();
}

size_t Serializer::getDataSize() const 
{
    return output.getDataSize();
}

bool Serializer::getNextRow(warp::StringRange & row) const
{
    if(rows.empty()) return false;

    if(!row)
    {
        row = rows.front();
        return true;
    }

    frag_vec::const_iterator i = std::upper_bound(
        rows.begin(), rows.end(), row, warp::less());

    if(i != rows.end())
    {
        row = *i;
        return true;
    }

    return false;
}

void Serializer::runLoop()
{
    lock_t lock(mutex);
    
    while(!quit)
    {
        //Tablet * t = server->getSerializeableTable();
    /*
        if(tablesForSerializer.empty()) 
        {
            for(table_map::const_iterator i = tableMap.begin();
                i != tableMap.end(); ++i) 
            {
                tablesForSerializer.push_back(i->second);
            }
        }
        }

        if(tablesForSerializer.empty()) continue;

        Table * t = tablesForSerializer.back();
        t->serialize(this);
    */

        cond.wait(lock);
    }
}

void Serializer::stop()
{
    quit = true;
    wake();
}

void Serializer::wake()
{
    cond.notify_one();
}
