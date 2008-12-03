//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-14
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

#ifndef KDI_TABLET_TABLET_H
#define KDI_TABLET_TABLET_H

#include <kdi/tablet/forward.h>
#include <kdi/table.h>
#include <warp/interval.h>
#include <warp/synchronized.h>
#include <ex/exception.h>

#include <boost/enable_shared_from_this.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>

#include <string>
#include <vector>
#include <list>
#include <map>
#include <set>

namespace kdi {
namespace tablet {

    class Tablet;

    EX_DECLARE_EXCEPTION(RowNotInTabletError, ex::RuntimeError);

} // namespace tablet
} // namespace kdi


//----------------------------------------------------------------------------
// Tablet
//----------------------------------------------------------------------------
class kdi::tablet::Tablet
    : public kdi::Table,
      public boost::enable_shared_from_this<kdi::tablet::Tablet>,
      private boost::noncopyable
{
    typedef boost::mutex mutex_t;
    typedef mutex_t::scoped_lock lock_t;

    typedef std::vector<FragmentPtr> fragments_t;
    typedef std::vector<ScannerWeakPtr> scanner_vec_t;

private:
    // These are initialized by the constructor and never change.
    // They may be accessed in a thread-safe manner.
    ConfigManagerPtr       const configMgr;
    SharedLoggerPtr        const logger;
    SharedCompactorPtr     const compactor;
    FileTrackerPtr         const tracker;
    WorkQueuePtr           const workQueue;
    SuperTablet *          const superTablet;
    std::string            const tableName;
    std::string            const server;
    std::string            const prettyName;

    // The following may change over the life of the Tablet.  Accesses
    // to them should be synchronized by holding a lock on the Tablet
    // mutex.
    warp::Interval<std::string>  rows;
    fragments_t                  fragments;
    std::vector<std::string>     deadFiles;
    bool                         mutationsPending;
    bool                         configChanged;
    bool                         splitPending;
    bool                         isCompacting;

    std::map<FragmentPtr, TabletPtr> clonedLogs;
    std::set<FragmentPtr>            duplicatedLogs;

    mutable mutex_t mutex;
    mutable warp::Synchronized<scanner_vec_t> syncScanners;

public:
    Tablet(std::string const & tableName,
           ConfigManagerPtr const & configMgr,
           SharedLoggerPtr const & logger,
           SharedCompactorPtr const & compactor,
           FileTrackerPtr const & tracker,
           WorkQueuePtr const & workQueue,
           TabletConfig const & cfg,
           SuperTablet * superTablet = 0);
    ~Tablet();

    // Table API
    void set(strref_t row, strref_t column, int64_t timestamp, strref_t value);
    void erase(strref_t row, strref_t column, int64_t timestamp);
    void sync();
    CellStreamPtr scan(ScanPredicate const & pred) const;
    
    /// Get the name of table of which this Tablet is a part
    std::string const & getTableName() const { return tableName; }

    /// Get a name suitable for printing in debug messages
    std::string const & getPrettyName() const { return prettyName; }

    /// Get a merged scan of all the tables in this this Tablet, using
    /// the given predicate.  This method does not support history
    /// predicates.
    CellStreamPtr getMergedScan(ScanPredicate const & pred) const;

    /// Add a new fragment to the Tablet.
    void addFragment(FragmentPtr const & fragment);

    /// Replace a sequence of ragments with a new fragment.
    void replaceFragments(std::vector<FragmentPtr> const & oldFragments,
                          FragmentPtr const & newFragment);

    /// Get the priority for compacting this Tablet.  Higher
    /// priorities are more important.  Priorities under 2 are
    /// ignored.
    size_t getCompactionPriority() const;

    /// Perform a Tablet compaction.  This method should only be
    /// called by the Compact thread.
    void doCompaction();

    /// Split the tablet into two.  An approximate median row is
    /// chosen as a split point.  This tablet shrinks to the range
    /// between the split point and the end of the tablet.  A new
    /// tablet is created to handle the range between the beginning of
    /// the old tablet and the split point.  The new tablet is
    /// returned.  If the split fails because the entire tablet is one
    /// row and there is no median row, nothing happens to this tablet
    /// and the returned tablet pointer will be null.
    TabletPtr splitTablet();

    /// Get the row upper bound for this Tablet.  This will never
    /// change over the life of the tablet.
    warp::IntervalPoint<std::string> const & getLastRow() const
    {
        return rows.getUpperBound();
    }

    /// Get the current row range for this Tablet.  The upper bound is
    /// fixed for the life of the tablet, but the lower bound can
    /// change as the tablet splits.
    warp::Interval<std::string> getRows() const
    {
        // XXX need lock
        return rows;
    }

private:
    // Constructor for splitting
    Tablet(Tablet const & other, warp::Interval<std::string> const & rows);

    void postConfigChange(lock_t const & lock);
    void doSaveConfig();
    void saveConfig(lock_t & lock);

    std::vector<std::string> getFragmentUris(lock_t const & lock) const;

    size_t getDiskSize(lock_t const & lock) const;
    std::string chooseSplitRow(lock_t & lock) const;

    /// Call reopen() on all Scanners.  Expired scanners will be
    /// filtered out of list as well.
    void updateScanners();

    /// Make sure row is in tablet range or raise a
    /// RowNotInTabletError
    inline void validateRow(strref_t row) const;

    /// Make sure predicate row set is in tablet range or raise a
    /// RowNotInTabletError
    inline void validateRows(ScanPredicate const & pred) const;
};


#endif // KDI_TABLET_TABLET_H
