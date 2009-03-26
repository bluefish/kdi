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

#include <kdi/net/TableLocator.h>
#include <kdi/net/TableManagerI.h>
#include <warp/StatTracker.h>
#include <warp/log.h>
#include <cassert>

using namespace kdi::net;
using warp::log;

//----------------------------------------------------------------------------
// TableLocator
//----------------------------------------------------------------------------
TableLocator::TableLocator(table_maker_t const & makeTable,
                           ScannerLocator * scannerLocator,
                           warp::StatTracker * tracker) :
    makeTable(makeTable),
    scannerLocator(scannerLocator),
    tracker(tracker)
{
}

TableLocator::~TableLocator()
{
}

Ice::ObjectPtr TableLocator::locate(Ice::Current const & cur,
                                    Ice::LocalObjectPtr & cookie)
{
    std::string const & name = cur.id.name;

    lock_t lock(mutex);
    for(;;)
    {
        // Get the locator item for the object name
        Item & item = objMap[name];

        // Handle the easy item states
        if(item.state == Item::READY)
        {
            // Either the object is loaded and ready to go, or we
            // failed to load before and the object is null.
            return item.obj;
        }
        else if(item.state == Item::LOADING)
        {
            // Another thread is currently loading this object.  Wait
            // for it to complete.
            log("TableLocator: waiting: %s", name);
            objectCreated.wait(lock);
            log("TableLocator: done waiting: %s", name);
            continue;
        }


        // We're the first to look for this item.  Start loading the
        // table.
        assert(item.state == Item::EMPTY);
        item.state = Item::LOADING;

        // Unlock while creating the new item
        lock.unlock();
        Ice::ObjectPtr obj = 0;
        try {
            // Create the table
            log("TableLocator: creating new table: %s", name);
            kdi::TablePtr tbl = makeTable(name);

            // Create the Ice object wrapper for the table
            using kdi::net::details::TableI;
            obj = new TableI(tbl, name, scannerLocator, tracker);
            log("TableLocator: created table %s", name);
        }
        catch(std::exception const & ex) {
            // Something broke
            log("TableLocator: failed to create %s: %s", name, ex.what());
        }
        catch(...) {
            // Something broke
            log("Locator: failed to create %s: unknown exception", name);
        }

        // Lock and update object map entry
        lock.lock();
        item.obj = obj;
        item.state = Item::READY;

        // Notify others that we're done loading
        objectCreated.notify_all();

        // Return new object (or null if the load failed)
        return item.obj;
    }
}

void TableLocator::finished(Ice::Current const & cur,
                            Ice::ObjectPtr const & servant,
                            Ice::LocalObjectPtr const & cookie)
{
}

void TableLocator::deactivate(std::string const & category)
{
    lock_t lock(mutex);
    objMap.clear();
}
