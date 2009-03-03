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

#include <kdi/server/Fragment.h>
#include <warp/interval.h>
#include <boost/thread/mutex.hpp>
#include <string>
#include <vector>
#include <memory>

namespace kdi {
namespace server {

    class Table;

    // Forward declarations
    class CellBuffer;
    class FragmentEventListener;
    class TabletEventListener;

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
    /// Make sure that all of the given rows are currently loaded in
    /// this Table and their max commit number is less than or equal
    /// to maxTxn.
    void verifyCommitApplies(std::auto_ptr<FragmentRowIterator> rows,
                             int64_t maxTxn) const;

    /// Update the committed transaction number for the given rows.
    void updateRowCommits(std::auto_ptr<FragmentRowIterator> rows,
                          int64_t commitTxn);

    void addFragmentListener(FragmentEventListener * listener);
    void removeFragmentListener(FragmentEventListener * listener);

    void addTabletListener(TabletEventListener * listener);
    void removeTabletListener(TabletEventListener * listener);

    /// Get the last transaction committed to this Table.
    int64_t getLastCommitTxn() const;

    /// Get the ordered chain of fragments to merge for the first part
    /// of the given predicate.  Throws and error if such a chain is
    /// not available (maybe the tablet corresponding to the first
    /// part of the range is not loaded on this server).  When the
    /// tablet is available, the fragment chain is returned in proper
    /// merge order.  The row interval over which the chain is valid
    /// is also returned.
    void getFirstFragmentChain(ScanPredicate const & pred,
                               std::vector<Fragment const *> & chain,
                               warp::Interval<std::string> & rows) const;
};

#endif // KDI_SERVER_TABLE_H
