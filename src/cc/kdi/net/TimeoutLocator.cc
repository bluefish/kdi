//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-02-11
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

#include <kdi/net/TimeoutLocator.h>
#include <kdi/net/TableManagerI.h>
#include <warp/log.h>
#include <boost/bind.hpp>
#include <iostream>

using namespace kdi;
using namespace kdi::net;
using namespace warp;
using namespace std;

//----------------------------------------------------------------------------
// TimeoutLocator
//----------------------------------------------------------------------------
void TimeoutLocator::cleanup()
{
    lock_t lock(mutex);
    kdi::Timestamp now = kdi::Timestamp::now();

    map_t::iterator it = objMap.begin();
    while(it != objMap.end())
    {
        map_t::iterator cur = it;
        ++it;

        kdi::Timestamp expiration = cur->second->getLastAccess() +
            kdi::Timestamp::fromSeconds(cur->second->timeoutSeconds);

        if(cur->second->refCount == 0 && now > expiration
           // XXX bug: TableI expiriation is causing multiple Tablets
           // to exist in the same directory simultaneously.
           && cur->first.category != "table"
            )
        {
            log("Locator expired: %s %s expiration=%s ref=%d",
                cur->first.category,
                cur->first.name,
                expiration,
                cur->second->refCount);

            objMap.erase(cur);
        }
        else
        {
            //log("Locator pending: %s %s expiration=%s ref=%d",
            //    cur->first.category,
            //    cur->first.name,
            //    expiration,
            //    cur->second->refCount);
        }
    }
    workQ.callLater(90, boost::bind(&TimeoutLocator::cleanup, this));
}

TimeoutLocator::TimeoutLocator(table_maker_t const & makeTable) :
    makeTable(makeTable),
    workQ(1)
{
    //log("TimeoutLocator %p: created", this);
    workQ.callLater(90, boost::bind(&TimeoutLocator::cleanup, this));
}

TimeoutLocator::~TimeoutLocator()
{
    //log("TimeoutLocator %p: destroyed", this);
}

Ice::ObjectPtr TimeoutLocator::locate(Ice::Current const & cur,
                                      Ice::LocalObjectPtr & cookie)
{
    lock_t lock(mutex);

    map_t::iterator it = objMap.find(cur.id);
    if(it == objMap.end())
    {
        if(cur.id.category != "table")
            return 0;

        log("Locator: new table %s", cur.id.name);
        
        // Open a new table
        using namespace kdi::net::details;
        kdi::TablePtr tbl = makeTable(cur.id.name);
        TableIPtr obj = new TableI(tbl, this);

        ItemPtr item(new Item<TableI>(obj, 600));
        it = objMap.insert(make_pair(cur.id, item)).first;
    }

    assert(it != objMap.end());
    ++(it->second->refCount);
    return it->second->getObject();
}

void TimeoutLocator::finished(Ice::Current const & cur,
                              Ice::ObjectPtr const & servant,
                              Ice::LocalObjectPtr const & cookie)
{
    lock_t lock(mutex);
    
    map_t::iterator it = objMap.find(cur.id);
    if(it != objMap.end())
        --(it->second->refCount);
}

void TimeoutLocator::deactivate(std::string const & category)
{
    lock_t lock(mutex);
    objMap.clear();
}
