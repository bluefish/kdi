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
#include <ex/exception.h>

#include <boost/enable_shared_from_this.hpp>
#include <boost/noncopyable.hpp>

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
    std::string name;

    ConfigManagerPtr configMgr;
    SharedLoggerSyncPtr syncLogger;
    SharedCompactorPtr compactor;

    std::string server;
    warp::Interval<std::string> rows;

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
    Tablet(std::string const & name,
           ConfigManagerPtr const & configMgr,
           SharedLoggerSyncPtr const & syncLogger,
           SharedCompactorPtr const & compactor);
    Tablet(std::string const & name,
           ConfigManagerPtr const & configMgr,
           SharedLoggerSyncPtr const & syncLogger,
           SharedCompactorPtr const & compactor,
           TabletConfig const & cfg);
    ~Tablet();

    // Table API
    void set(strref_t row, strref_t column, int64_t timestamp, strref_t value);
    void erase(strref_t row, strref_t column, int64_t timestamp);
    void sync();
    CellStreamPtr scan() const;
    CellStreamPtr scan(ScanPredicate const & pred) const;
    
    /// Get the name of this Tablet
    std::string const & getName() const { return name; }

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

private:
    void loadConfig(TabletConfig const & cfg);
    void saveConfig() const;

    /// Call reopen() on all Scanners.  Expired scanners will be
    /// filtered out of list as well.
    void updateScanners() const;
};


#endif // KDI_TABLET_TABLET_H
