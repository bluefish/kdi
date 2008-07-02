//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/net/TimeoutLocator.h $
//
// Created 2008/02/11
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_NET_TIMEOUTLOCATOR_H
#define KDI_NET_TIMEOUTLOCATOR_H

#include <warp/call_queue.h>
#include <kdi/timestamp.h>
#include <kdi/table.h>
#include <Ice/ServantLocator.h>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>
#include <map>

namespace kdi {
namespace net {

    class TimeoutLocator;
    typedef IceUtil::Handle<TimeoutLocator> TimeoutLocatorPtr;

} // namespace net
} // namespace kdi


//----------------------------------------------------------------------------
// TimeoutLocator
//----------------------------------------------------------------------------
class kdi::net::TimeoutLocator
    : public Ice::ServantLocator
{
    class ItemBase
    {
    public:
        double timeoutSeconds;
        int refCount;

        explicit ItemBase(double timeoutSeconds) :
            timeoutSeconds(timeoutSeconds),
            refCount(0)
        {}
        virtual ~ItemBase() {}
        virtual Ice::ObjectPtr getObject() const = 0;
        virtual kdi::Timestamp getLastAccess() const = 0;
    };
    typedef boost::shared_ptr<ItemBase> ItemPtr;

    template <class T>
    class Item : public ItemBase
    {
        IceUtil::Handle<T> ptr;
    public:
        Item(IceUtil::Handle<T> const & ptr, double timeoutSeconds) :
            ItemBase(timeoutSeconds), ptr(ptr) {}
        virtual Ice::ObjectPtr getObject() const { return ptr; }
        virtual kdi::Timestamp getLastAccess() const {
            return ptr->getLastAccess();
        }
    };

    typedef boost::mutex mutex_t;
    typedef mutex_t::scoped_lock lock_t;
    typedef std::map<Ice::Identity, ItemPtr> map_t;
    typedef boost::function<kdi::TablePtr (std::string const &)> table_maker_t;

    table_maker_t makeTable;
    map_t objMap;
    mutex_t mutex;
    warp::CallQueue workQ;

    void cleanup();

public:
    explicit TimeoutLocator(table_maker_t const & makeTable);
    ~TimeoutLocator();

    template <class T>
    void add(Ice::Identity const & id, IceUtil::Handle<T> const & ptr,
             double timeoutSeconds)
    {
        lock_t lock(mutex);
        objMap[id].reset(new Item<T>(ptr, timeoutSeconds));
    }

    void remove(Ice::Identity const & id)
    {
        lock_t lock(mutex);
        objMap.erase(id);
    }

    virtual Ice::ObjectPtr locate(Ice::Current const & cur,
                                  Ice::LocalObjectPtr & cookie);
    virtual void finished(Ice::Current const & cur,
                          Ice::ObjectPtr const & servant,
                          Ice::LocalObjectPtr const & cookie);
    virtual void deactivate(std::string const & category);
};

#endif // KDI_NET_TIMEOUTLOCATOR_H
