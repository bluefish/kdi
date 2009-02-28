//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/server/FragmentEventListener.h $
//
// Created 2009/02/27
//
// Copyright 2009 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_SERVER_FRAGMENTEVENTLISTENER_H
#define KDI_SERVER_FRAGMENTEVENTLISTENER_H

#include <kdi/server/Fragment.h>
#include <warp/interval.h>
#include <string>
#include <vector>

namespace kdi {
namespace server {

    class FragmentEventListener;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// FragmentEventListener
//----------------------------------------------------------------------------
class kdi::server::FragmentEventListener
{
public:
    /// Event: a fragment 'f' has been created for the range 'r'.
    virtual void onNewFragment(
        warp::Interval<std::string> const & r,
        Fragment const * f) = 0;

    /// Event: for the range 'r', all of the fragments in 'f1' have
    /// been replaced by fragment 'f2'.  Note that 'f2' may be null,
    /// meaning that the 'f1' fragments have simply been removed.
    virtual void onReplaceFragments(
        warp::Interval<std::string> const & r,
        std::vector<Fragment const *> const & f1,
        Fragment const * f2);

protected:
    ~FragmentEventListener() {}
};

#endif // KDI_SERVER_FRAGMENTEVENTLISTENER_H
