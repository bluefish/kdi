//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-02-11
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

        if(!cur->second)
        {
            // Pointer is null, it's in mid creation
            continue;
        }

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
    for(;;)
    {
        // Look up ID in object map
        map_t::iterator it = objMap.find(cur.id);

        // Did we find the object?
        if(it != objMap.end())
        {
            // Object is in the map...
            if(it->second)
            {
                // ...and is non-null.  Return it.
                ++(it->second->refCount);
                return it->second->getObject();
            }
            else
            {
                // ...and is null.  Another thread is trying to create
                // it.  Wait.
                log("Locator: waiting for object: %s", cur.id.name);
                objectCreated.wait(lock);
                log("Locator: done waiting for object: %s", cur.id.name);

                // Try again
                continue;
            }
        }

        // Object is not in the map -- maybe we can create it

        // We can only create tables
        if(cur.id.category != "table")
            return 0;

        // Insert a null entry into the map to signal that we're going
        // to try to create it
        it = objMap.insert(make_pair(cur.id, ItemPtr())).first;

        // Unlock while creating the new item
        lock.unlock();
        ItemPtr item;
        try {
            // Create the table
            log("Locator: creating new table: %s", cur.id.name);
            kdi::TablePtr tbl = makeTable(cur.id.name);

            // Create an item for the table
            using namespace kdi::net::details;
            TableIPtr obj = new TableI(tbl, this);
            item.reset(new Item<TableI>(obj, 600));
        }
        catch(...) {
            // Something broke -- lock and clean up before bailing
            log("Locator: creation failed: %s", cur.id.name);
            lock.lock();
            objMap.erase(it);
            objectCreated.notify_all();
            throw;
        }

        // Lock and update object map entry
        lock.lock();
        log("Locator: created table: %s", cur.id.name);
        it->second = item;

        // Notify others that we're done
        objectCreated.notify_all();

        // Return new object 
        ++(it->second->refCount);
        return it->second->getObject();
    }
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
