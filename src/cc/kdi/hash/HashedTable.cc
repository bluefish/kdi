//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-10-24
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

#include <kdi/hash/HashedTable.h>
#include <kdi/cell_merge.h>
#include <kdi/scan_predicate.h>
#include <warp/hsieh_hash.h>
#include <ex/exception.h>
#include <boost/algorithm/string.hpp>

using namespace kdi;
using namespace kdi::hash;

//----------------------------------------------------------------------------
// HashedTable
//----------------------------------------------------------------------------

TablePtr const & HashedTable::pick(strref_t row)
{
    uint32_t h = warp::hsieh_hash(row.begin(), row.end());
    TableInfo & i = tables[h % tables.size()];
    i.isDirty = true;
    return i.table;
}

HashedTable::HashedTable(std::vector<TablePtr> const & tables)
{
    using namespace ex;

    if(tables.empty())
        raise<ValueError>("empty table vector");

    for(std::vector<TablePtr>::const_iterator i = tables.begin();
        i != tables.end(); ++i)
    {
        this->tables.push_back(TableInfo(*i));
    }
}

void HashedTable::set(strref_t row, strref_t column, int64_t timestamp,
                      strref_t value)
{
    pick(row)->set(row, column, timestamp, value);
}

void HashedTable::erase(strref_t row, strref_t column, int64_t timestamp)
{
    pick(row)->erase(row, column, timestamp);
}

void HashedTable::insert(Cell const & x)
{
    pick(x.getRow())->insert(x);
}

CellStreamPtr HashedTable::scan(ScanPredicate const & pred) const
{
    CellStreamPtr merge = CellMerge::make(false);
    for(std::vector<TableInfo>::const_iterator i = tables.begin();
        i != tables.end(); ++i)
    {
        merge->pipeFrom(i->table->scan(pred));
    }
    return merge;
}

void HashedTable::sync()
{
    for(std::vector<TableInfo>::iterator i = tables.begin();
        i != tables.end(); ++i)
    {
        if(i->isDirty)
        {
            i->table->sync();
            i->isDirty = false;
        }
    }
}

RowIntervalStreamPtr HashedTable::scanIntervals() const
{
    if(!tables.empty())
        return tables.front().table->scanIntervals();
    else
        return kdi::Table::scanIntervals();
}


//----------------------------------------------------------------------------
// Registration
//----------------------------------------------------------------------------
#include <warp/init.h>
#include <warp/uri.h>
#include <warp/varsub.h>
#include <kdi/table_factory.h>
#include <map>
#include <algorithm>

namespace
{
    kdi::TablePtr openHashed(std::string const & uri)
    {
        using namespace warp;
        using namespace ex;
        using namespace std;
        using boost::algorithm::starts_with;

        Uri u(uri);

        StringRange key;
        StringRange value;
        for(UriQuery q(u.query); q.hasAttr(); q.next())
        {
            if(starts_with(q.key, "hash_") && q.key.size() > 5)
            {
                if(!key.empty())
                    raise<ValueError>("hash URI has multiple keys");

                key = q.key;
                value = q.value;
            }
        }
        if(key.empty())
            raise<ValueError>("hash URI has no key");

        string pattern = uriEraseParameter(u.popScheme(), key);
        map<string, string> vars;
        string & sub = vars[string(key.begin()+5, key.end())];
        
        vector<TablePtr> tables;
        for(char const * p = value.begin(); p != value.end();)
        {
            char const * q = std::find(p, value.end(), ',');
            sub.assign(p, q);
            tables.push_back(Table::open(varsub(pattern, vars)));

            if(q == value.end())
                break;
            p = q + 1;
        }

        kdi::TablePtr p(new kdi::hash::HashedTable(tables));
        return p;
    }
}

WARP_DEFINE_INIT(kdi_hash_HashedTable)
{
    kdi::TableFactory::get().registerTable(
        "hash", &openHashed
        );
}

