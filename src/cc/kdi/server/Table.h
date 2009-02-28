//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/server/Table.h $
//
// Created 2009/02/27
//
// Copyright 2009 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_SERVER_TABLE_H
#define KDI_SERVER_TABLE_H

#include <warp/interval.h>
#include <boost/thread/mutex.hpp>
#include <string>
#include <vector>

namespace kdi {
namespace server {

    class Table;

    // Forward declarations
    class CellBuffer;
    class FragmentEventListener;
    class TabletEventListener;
    class Fragment;

} // namespace server

    // Forward declarations
    class ScanPredicate;

} // namespace kdi

//----------------------------------------------------------------------------
// Table
//----------------------------------------------------------------------------
class kdi::server::Table
{
public:
    boost::mutex tableMutex;

public:
    void verifyTabletsAreLoaded(CellBuffer const * cells) const;
    void verifyCommitApplies(CellBuffer const * cells, int64_t maxTxn) const;
    void updateRowCommits(CellBuffer const * cells, int64_t commitTxn);

    void addFragmentListener(FragmentEventListener * listener);
    void removeFragmentListener(FragmentEventListener * listener);

    void addTabletListener(TabletEventListener * listener);
    void removeTabletListener(TabletEventListener * listener);

    int64_t getLastCommitTxn() const;

    void getFirstFragmentChain(ScanPredicate const & pred,
                               std::vector<Fragment const *> & chain,
                               warp::Interval<std::string> & rows) const;
};

#endif // KDI_SERVER_TABLE_H
