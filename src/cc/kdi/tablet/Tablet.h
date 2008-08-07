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

    std::string tableName;

    ConfigManagerPtr configMgr;
    SharedLoggerPtr logger;
    SharedCompactorPtr compactor;
    SuperTablet * superTablet;

    std::string server;
    warp::Interval<std::string> rows;

    bool mutationsPending;
    
    struct ConfigInfo
    {
        bool configChanged;
        std::vector<std::string> deadFiles;

        ConfigInfo() : configChanged(false) {}
    };
    warp::Synchronized<ConfigInfo> syncConfigInfo;

    struct TableInfo
    {
        TablePtr table;
        std::string uri;
        bool isStatic;

        TableInfo(TablePtr const & table,
                  std::string const & uri,
                  bool isStatic) :
            table(table),
            uri(uri),
            isStatic(isStatic) {}
        
        bool operator==(TablePtr const & tbl) const {
            return table == tbl;
        }
    };

    typedef std::list<TableInfo> tables_t;
    warp::Synchronized<tables_t> syncTables;

    typedef std::vector<ScannerWeakPtr> scanner_vec_t;
    mutable warp::Synchronized<scanner_vec_t> syncScanners;

    
public:
    Tablet(std::string const & tableName,
           ConfigManagerPtr const & configMgr,
           SharedLoggerPtr const & logger,
           SharedCompactorPtr const & compactor,
           TabletConfig const & cfg,
           SuperTablet * superTablet = 0);
    ~Tablet();

    // Table API
    void set(strref_t row, strref_t column, int64_t timestamp, strref_t value);
    void erase(strref_t row, strref_t column, int64_t timestamp);
    void sync();
    CellStreamPtr scan() const;
    CellStreamPtr scan(ScanPredicate const & pred) const;
    
    /// Get the name of table of which this Tablet is a part
    std::string const & getTableName() const { return tableName; }

    /// Get a name suitable for printing in debug messages
    std::string getPrettyName() const;

    /// Get a merged scan of all the tables in this this Tablet, using
    /// the given predicate.  This method does not support history
    /// predicates.
    CellStreamPtr getMergedScan(ScanPredicate const & pred) const;

    /// Add a new table to the Tablet.  The tableUri can be used to
    /// reload the table and is suitable for state serialization.
    void addTable(TablePtr const & table, std::string const & tableUri);

    /// Replace a sequence of tables with a new table.  The URI can be
    /// used to reload the new table.
    void replaceTables(std::vector<TablePtr> const & oldTables,
                       TablePtr const & newTable,
                       std::string const & newTableUri);

    /// Get the priority for compacting this Tablet.  Higher
    /// priorities are more important.  Priorities under 2 are
    /// ignored.
    size_t getCompactionPriority() const;

    /// Perform a Tablet compaction.  This method should only be
    /// called by the Compact thread.
    void doCompaction();

    /// Get the row range served by this Tablet.
    warp::Interval<std::string> const & getRows() const
    {
        return rows;
    }

    /// Split the tablet into two.  An approximate median row is
    /// chosen as a split point.  This tablet shrinks to the range
    /// between the split point and the end of the tablet.  A new
    /// tablet is created to handle the range between the beginning of
    /// the old tablet and the split point.  The new tablet is
    /// returned.  If the split fails because the entire tablet is one
    /// row and there is no median row, nothing happens to this tablet
    /// and the returned tablet pointer will be null.
    TabletPtr splitTablet();

private:
    // Copy constructor for splitting
    Tablet(Tablet const & other);

    void loadConfig(TabletConfig const & cfg);
    void saveConfig() const;

    std::vector<std::string> getFragmentUris() const;

    size_t getDiskSize() const;
    std::string chooseSplitRow() const;

    /// Call reopen() on all Scanners.  Expired scanners will be
    /// filtered out of list as well.
    void updateScanners() const;
};


#endif // KDI_TABLET_TABLET_H
