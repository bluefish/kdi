//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-07-14
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

#ifndef KDI_TABLET_SUPERTABLET_H
#define KDI_TABLET_SUPERTABLET_H

#include <kdi/table.h>
#include <kdi/tablet/forward.h>
#include <kdi/tablet/MetaConfigManager.h>
#include <ex/exception.h>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/noncopyable.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <vector>

namespace kdi {
namespace tablet {

    /// A collection of Tablets in the same table.
    class SuperTablet;

    EX_DECLARE_EXCEPTION(TableDoesNotExistError, ex::RuntimeError);

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// SuperTablet
//----------------------------------------------------------------------------
class kdi::tablet::SuperTablet
    : public kdi::Table,
      public boost::enable_shared_from_this<kdi::tablet::SuperTablet>,
      private boost::noncopyable
{
    typedef boost::mutex mutex_t;
    typedef mutex_t::scoped_lock lock_t;

    WorkQueuePtr workQueue;

    std::vector<TabletPtr> tablets;
    mutable std::vector<SuperScannerWeakPtr> scanners;
    mutable mutex_t scannerMutex;

    bool mutationsBlocked;
    size_t mutationsPending;

    mutable mutex_t mutex;
    boost::condition allQuiet;
    boost::condition allowMutations;

    class MutationInterlock;
    class MutationLull;

public:
    SuperTablet(std::string const & name,
                MetaConfigManagerPtr const & configMgr,
                FragmentLoader * loader,
                SharedLoggerPtr const & logger,
                SharedCompactorPtr const & compactor,
                FileTrackerPtr const & tracker,
                WorkQueuePtr const & workQueue);
    ~SuperTablet();

    // Table API
    virtual void set(strref_t row, strref_t column, int64_t timestamp,
                     strref_t value);
    virtual void erase(strref_t row, strref_t column, int64_t timestamp);
    virtual void insert(Cell const & x);
    virtual CellStreamPtr scan(ScanPredicate const & pred) const;
    virtual void sync();

    /// Add the tablet to the split queue.
    void requestSplit(Tablet * tablet);

    /// Split the given tablet.  This function cannot be called from
    /// the Table API threads or we'll get deadlocked.
    void performSplit(Tablet * tablet);

    /// Scan the first tablet matching the row predicate.  The scan on
    /// the tablet will be clipped to the tablet's row range.  The
    /// tablet row range will be storted in tabletRows, if non-null.
    CellStreamPtr scanFirstTablet(ScanPredicate const & pred,
                                  warp::Interval<std::string> * tabletRows) const;

private:
    /// Get the Tablet containing the given row
    TabletPtr const & getTablet(strref_t row) const;

    /// Call reopen() on all Scanners.  Expired scanners will be
    /// filtered out of list as well.
    void updateScanners();
};


#endif // KDI_TABLET_SUPERTABLET_H
