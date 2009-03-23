//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-23
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

#include <kdi/mux/MultiTable.h>
#include <kdi/scan_predicate.h>
#include <kdi/cell_merge.h>
#include <warp/string_interval.h>
#include <warp/config.h>
#include <ex/exception.h>
#include <boost/tokenizer.hpp>

using namespace kdi;
using namespace kdi::mux;
using namespace ex;

namespace {

    typedef boost::char_separator<char> sep_t;
    typedef boost::tokenizer<sep_t> tok_t;

    warp::StringRange getColumnFamily(strref_t col)
    {
        char const * p = std::find(col.begin(), col.end(), ':');
        if(p == col.end())
            p = col.begin();
        return warp::StringRange(col.begin(), p);
    }

}

//----------------------------------------------------------------------------
// MultiTable::Group
//----------------------------------------------------------------------------
CellStreamPtr MultiTable::Group::maybeScan(ScanPredicate const & pred) const
{
    ScanPredicate::StringSetCPtr pcols = pred.getColumnPredicate();
    if(!pcols)
        return openScan(ScanPredicate(pred).setColumnPredicate(columns));

    warp::IntervalSet<std::string> x(columns);
    x.intersect(*pcols);
    if(!x.isEmpty())
        return openScan(ScanPredicate(pred).setColumnPredicate(x));
    else
        return CellStreamPtr();
}

CellStreamPtr MultiTable::Group::openScan(ScanPredicate const & pred) const
{
    if(!table)
        table = Table::open(uri);
    return table->scan(pred);
}

//----------------------------------------------------------------------------
// MultiTable
//----------------------------------------------------------------------------
MultiTable::Group * MultiTable::findColumnGroup(strref_t col) const
{
    strref_t fam = getColumnFamily(col);
    column_map::const_iterator i = index.find(fam);
    if(i != index.end())
        return i->second;

    if(defaultGroup)
        return defaultGroup;

    raise<RuntimeError>("no group for column: %s", col);
}

MultiTable::Group * MultiTable::openColumnGroup(strref_t col)
{
    Group * g = findColumnGroup(col);
    if(!g->table)
        g->table = Table::open(g->uri);
    return g;
}

MultiTable::MultiTable(warp::Config const & cfg)
{
    loadConfig(cfg);
}

void MultiTable::set(strref_t r, strref_t c, int64_t t, strref_t v)
{
    Group * g = openColumnGroup(c);
    g->table->set(r, c, t, v);
    g->dirty = true;
}

void MultiTable::erase(strref_t r, strref_t c, int64_t t)
{
    Group * g = openColumnGroup(c);
    g->table->erase(r, c, t);
    g->dirty = true;
}

void MultiTable::sync()
{
    for(group_vec::iterator i = groups.begin();
        i != groups.end(); ++i)
    {
        if(i->dirty)
        {
            i->table->sync();
            i->dirty = false;
        }
    }
}

CellStreamPtr MultiTable::scan() const
{
    return scan(ScanPredicate());
}

CellStreamPtr MultiTable::scan(ScanPredicate const & pred) const
{
    std::vector<CellStreamPtr> scans;
    for(group_vec::const_iterator i = groups.begin();
        i != groups.end(); ++i)
    {
        CellStreamPtr s = i->maybeScan(pred);
        if(s)
            scans.push_back(s);
    }

    if(scans.size() == 1)
        return scans.front();
        
    CellStreamPtr merge = CellMerge::make(false);
    for(std::vector<CellStreamPtr>::const_iterator i = scans.begin();
        i != scans.end(); ++i)
    {
        merge->pipeFrom(*i);
    }

    return merge;
}

//#include <boost/format.hpp>
//#include <iostream>

void MultiTable::loadConfig(warp::Config const & cfg)
{
    // group.x.columns = foo bar
    // group.x.uri = kdi+hash://hcrawl00$x/foobar?hash_x=0,1,2,3,4
    // group.y.columns = baz
    // group.y.uri = kdi+hash://hcrawl00$x/baz?hash_x=0,1,2,3,4
    
    sep_t sep(" \r\n\t");

    warp::Config const & group = cfg.getChild("group");
    size_t nGroups = group.numChildren();

    groups.clear();
    index.clear();
    defaultGroup = 0;

    groups.resize(nGroups);
    for(size_t i = 0; i < nGroups; ++i)
    {
        warp::Config const & c = group.getChild(i);
        Group & g = groups[i];

        g.uri = c.get("uri");
        tok_t tok(c.get("columns"), sep);
        for(tok_t::iterator w = tok.begin(); w != tok.end(); ++w)
        {
            g.columns.add(warp::makePrefixInterval(*w + ":"));
            index[*w] = &g;
        }
    }

    // using namespace std;
    // for(group_vec::const_iterator i = groups.begin();
    //     i != groups.end(); ++i)
    // {
    //     cout << boost::format("Group %d: %s\n  %s")
    //         % (i - groups.begin())
    //         % i->uri
    //         % i->columns
    //          << endl;
    // }
    // for(column_map::const_iterator i = index.begin();
    //     i != index.end(); ++i)
    // {
    //     cout << boost::format("Family %s --> Group %d")
    //         % i->first
    //         % (i->second - &*groups.begin())
    //          << endl;
    // }
}


//----------------------------------------------------------------------------
// Registration
//----------------------------------------------------------------------------
#include <warp/init.h>
#include <warp/uri.h>
#include <kdi/table_factory.h>

namespace {

    TablePtr openMux(std::string const & uri)
    {
        TablePtr p(
            new MultiTable(
                warp::Config(
                    warp::uriPopScheme(uri))));
        return p;
    }

}

WARP_DEFINE_INIT(kdi_mux_MultiTable)
{
    TableFactory::get().registerTable("mux", &openMux);
}
