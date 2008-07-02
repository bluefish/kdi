//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/SharedCompactor.h $
//
// Created 2008/05/21
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_TABLET_SHAREDCOMPACTOR_H
#define KDI_TABLET_SHAREDCOMPACTOR_H

#include <kdi/tablet/forward.h>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <set>

namespace kdi {
namespace tablet {

    class SharedCompactor;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// SharedCompactor
//----------------------------------------------------------------------------
class kdi::tablet::SharedCompactor :
    private boost::noncopyable
{
    typedef std::set<TabletWeakPtr> set_t;
    typedef boost::mutex::scoped_lock lock_t;

    boost::mutex mutex;
    boost::condition requestAdded;
    boost::scoped_ptr<boost::thread> thread;

    set_t requests;
    bool cancel;

public:
    SharedCompactor();
    ~SharedCompactor();

    void requestCompaction(TabletPtr const & tablet);
    void shutdown();

private:
    bool getTabletForCompaction(TabletPtr & tablet);
    void compactLoop();
};

#endif // KDI_TABLET_SHAREDCOMPACTOR_H
