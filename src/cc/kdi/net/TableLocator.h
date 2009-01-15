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

#ifndef KDI_NET_TABLELOCATOR_H
#define KDI_NET_TABLELOCATOR_H

#include <kdi/table.h>
#include <Ice/ServantLocator.h>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/function.hpp>
#include <tr1/unordered_map>
#include <string>

namespace kdi {
namespace net {

    class TableLocator;
    typedef IceUtil::Handle<TableLocator> TableLocatorPtr;

    // Forward declaration
    class ScannerLocator;

} // namespace net
} // namespace kdi


//----------------------------------------------------------------------------
// TableLocator
//----------------------------------------------------------------------------
class kdi::net::TableLocator
    : public Ice::ServantLocator
{
    struct Item
    {
        enum State { EMPTY, LOADING, READY };

        State state;
        Ice::ObjectPtr obj;

        Item() : state(EMPTY), obj(0) {}
    };

    typedef boost::mutex mutex_t;
    typedef mutex_t::scoped_lock lock_t;

    typedef std::tr1::unordered_map<std::string, Item> map_t;
    typedef boost::function<kdi::TablePtr (std::string const &)> table_maker_t;

    table_maker_t makeTable;
    ScannerLocator * scannerLocator;

    map_t objMap;
    mutex_t mutex;
    boost::condition objectCreated;

public:
    TableLocator(table_maker_t const & makeTable,
                 ScannerLocator * scannerLocator);
    ~TableLocator();

    virtual Ice::ObjectPtr locate(Ice::Current const & cur,
                                  Ice::LocalObjectPtr & cookie);
    virtual void finished(Ice::Current const & cur,
                          Ice::ObjectPtr const & servant,
                          Ice::LocalObjectPtr const & cookie);
    virtual void deactivate(std::string const & category);
};

#endif // KDI_NET_TABLELOCATOR_H
