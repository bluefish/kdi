//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-21
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

#include <kdi/tablet/SharedCompactor.h>
#include <kdi/tablet/Tablet.h>
#include <warp/log.h>
#include <boost/bind.hpp>
#include <iostream>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace std;

//----------------------------------------------------------------------------
// SharedCompactor
//----------------------------------------------------------------------------
SharedCompactor::SharedCompactor() :
    cancel(false)
{
    thread.reset(
        new boost::thread(
            boost::bind(
                &SharedCompactor::compactLoop,
                this
                )
            )
        );
}
    
SharedCompactor::~SharedCompactor()
{
    shutdown();
}

void SharedCompactor::requestCompaction(TabletPtr const & tablet)
{
    lock_t sync(mutex);
    requests.insert(tablet);
    requestAdded.notify_one();
}

void SharedCompactor::shutdown()
{
    lock_t sync(mutex);
    if(!cancel)
    {
        log("SharedCompactor shutdown");
        cancel = true;
        requestAdded.notify_all();

        sync.unlock();
        thread->join();
    }
}

bool SharedCompactor::getTabletForCompaction(TabletPtr & tablet)
{
    lock_t sync(mutex);
    while(!cancel)
    {
        // Wait until we have at least one pending request
        if(requests.empty())
        {
            log("Compact thread: waiting for compaction request");
            requestAdded.wait(sync);
            continue;
        }

        // Find the tablet most in need of a compaction
        log("Compact thread: finding highest priority compaction");
        TabletPtr maxTablet;
        size_t maxPriority = 0;
        set_t::iterator i = requests.begin();
        while(i != requests.end())
        {
            // Get compaction priority for each Tablet in the
            // request set.  Some of the Tablets may have expired
            // and some may no longer want compactions.  A null
            // pointer indicates the former.  A priority of zero
            // indicates the latter.
            TabletPtr t = i->lock();
            size_t pri = 0;
            if(t && 0 < (pri = t->getCompactionPriority()))
            {
                // Got a Tablet with non-zero priority.  Update
                // our max.
                if(!maxTablet || pri > maxPriority)
                {
                    maxTablet = t;
                    maxPriority = pri;
                }
                ++i;
            }
            else
            {
                // Tablet has gone away or doesn't want a
                // compaction anymore.  Remove it from the request
                // list.
                requests.erase(i++);
            }
        }

        // If we found a Tablet, return it
        if(maxTablet)
        {
            log("Compact thread: selected %s (pri=%d)",
                maxTablet->getPrettyName(), maxPriority);
            tablet = maxTablet;
            return true;
        }

        // Loop and try again
    }

    // Canceled
    log("Compact thread: selection canceled");
    return false;
}

void SharedCompactor::compactLoop()
{
    // XXX what should we do when this thread crashes?

    log("Compact thread starting");

    try {
        for(TabletPtr tablet;
            getTabletForCompaction(tablet);
            tablet.reset())
        {
            tablet->doCompaction();
        }
    }
    catch(std::exception const & ex) {
        log("SharedCompactor error: %s", ex.what());
    }
    catch(...) {
        log("SharedCompactor error: unknown exception");
    }

    log("Compact thread exiting");
}
