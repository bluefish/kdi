//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-02-27
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

#ifndef KDI_SERVER_TABLE_H
#define KDI_SERVER_TABLE_H

#include <kdi/server/Fragment.h>
#include <kdi/server/FragmentEventListener.h>
#include <kdi/server/TableSchema.h>
#include <kdi/server/CommitRing.h>
#include <warp/interval.h>
#include <warp/hashmap.h>
#include <warp/strhash.h>
#include <boost/thread/mutex.hpp>
#include <boost/shared_ptr.hpp>
#include <tr1/unordered_set>
#include <string>
#include <vector>
#include <memory>

namespace kdi {
namespace server {

    class Table;
    class TableLock;

    enum TableState {
        TABLE_UNKNOWN,
        TABLE_LOADING_SCHEMA,
        TABLE_ACTIVE,
    };

    // Forward declarations
    class Tablet;
    class CellBuffer;
    class FragmentEventListener;
    class TabletEventListener;
    class Serializer;
    class Compactor;
    class FragmentLoader;
    class FragmentWriterFactory;
    class RangeFragmentMap;
    class TabletConfig;

} // namespace server

    // Forward declarations
    class ScanPredicate;

} // namespace kdi

//----------------------------------------------------------------------------
// Table
//----------------------------------------------------------------------------
class kdi::server::Table
{
private:
    friend class TableLock;
    mutable boost::mutex tableMutex;

public:
    /// Create a Table with a default schema
    explicit Table(std::string const & tableName);
    ~Table();

public:                         // Must hold TableLock
    /// Make sure that the tablets containing all the given rows are
    /// currently loaded in the table.  Tablets that are still loading
    /// are returned in the loadingTablets vector.
    void verifyTabletsLoaded(std::vector<warp::StringRange> const & rows,
                             std::vector<Tablet *> & loadingTablets) const;
    
    /// Make sure that the current schema for this table has a place
    /// for each of the given column families.
    void verifyColumnFamilies(std::vector<std::string> const & families) const;

    /// Make sure that all of the given rows have a commit number less
    /// than or equal to maxTxn.
    void verifyCommitApplies(std::vector<warp::StringRange> const & rows,
                             int64_t maxTxn) const;

    /// Update the committed transaction number for the given rows.
    void updateRowCommits(std::vector<warp::StringRange> const & rows,
                          int64_t commitTxn);

    void addFragmentListener(FragmentEventListener * listener)
    {
        fragmentListeners.insert(listener);
    }

    void removeFragmentListener(FragmentEventListener * listener)
    {
        fragmentListeners.erase(listener);
    }

    void addTabletListener(TabletEventListener * listener)
    {
        tabletListeners.insert(listener);
    }

    void removeTabletListener(TabletEventListener * listener)
    {
        tabletListeners.erase(listener);
    }

    /// Get the last transaction committed to this Table.
    int64_t getLastCommitTxn() const
    {
        return rowCommits.getMaxCommit();
    }

    /// Find a Tablet or return null.
    Tablet * findTablet(warp::IntervalPoint<std::string> const & lastRow) const;

    /// Get a Tablet or throw an exception.
    Tablet * getTablet(warp::IntervalPoint<std::string> const & lastRow) const;

    /// Create a new Tablet.
    Tablet * createTablet(warp::IntervalPoint<std::string> const & lastRow);

    /// Get the ordered chain of fragments to merge for the first part
    /// of the given predicate.  Throws and error if such a chain is
    /// not available (maybe the tablet corresponding to the first
    /// part of the range is not loaded on this server).  When the
    /// tablet is available, the fragment chain is returned in proper
    /// merge order.  The row interval over which the chain is valid
    /// is also returned.
    void getFirstFragmentChain(ScanPredicate const & pred,
                               std::vector<FragmentCPtr> & chain,
                               warp::Interval<std::string> & rows) const;

    void addMemoryFragment(FragmentCPtr const & frag, int64_t txn);

    void addLoadedFragments(warp::Interval<std::string> const & rows,
                            std::vector<FragmentCPtr> const & frags);

    void issueFragmentEvent(warp::Interval<std::string> const & affectedRows,
                            FragmentEventType type)
    {
        for(fel_set::const_iterator i = fragmentListeners.begin();
            i != fragmentListeners.end(); ++i)
        {
            (*i)->onFragmentEvent(affectedRows, type);
        }
    }

private:
    void getAllMemFrags(std::vector<FragmentCPtr> const & out);

    void getPredicateGroups(ScanPredicate const & pred,
                            std::vector<int> & out) const;

public:
    void applySchema(TableSchema const & s);

    TableSchema const & getSchema() const { return schema; }
    size_t getSchemaVersion() const { return schemaVersion; }

    TableState getState() const
    {
        if(!schemaVersion)
            return TABLE_LOADING_SCHEMA;
        else
            return TABLE_ACTIVE;
    }

    /// Append the mem fragment list for the given group to the output
    /// list.
    void getMemFragments(
        int groupIndex, std::vector<FragmentCPtr> & out) const;

    size_t getMemSize(int groupIndex) const;

    /// Get the earliest commit in the given group that is still in
    /// memory (not yet serialized).
    int64_t getEarliestMemCommit(int groupIndex) const;

    /// Get the earliest commit in any group that is still in memory
    /// (not yet serialized).
    int64_t getEarliestMemCommit() const;

    size_t getMaxDiskChainLength(int groupIndex) const;
    void getCompactionSet(int groupIndex, RangeFragmentMap & compactionSet) const;

    void replaceMemFragments(
        std::vector<FragmentCPtr> const & oldFragments,
        FragmentCPtr const & newFragment,
        int groupIndex,
        std::vector<std::string> const & rowCoverage,
        std::vector<Tablet *> & updatedTablets);

    void replaceDiskFragments(
        std::vector<FragmentCPtr> const & oldFragments,
        FragmentCPtr const & newFragment,
        int groupIndex,
        warp::Interval<std::string> const & rowRange,
        std::vector<Tablet *> & updatedTablets);

private:
    class TabletLt;

    typedef std::vector<FragmentCPtr> frag_vec;

    typedef std::pair<FragmentCPtr,int64_t> memfrag;
    typedef std::vector<memfrag> memfrag_vec;
    typedef std::vector<memfrag_vec> memfragvec_vec;

    typedef std::vector<Tablet *> tablet_vec;
    typedef warp::HashMap<std::string, int, warp::HsiehHash> group_map;

    typedef std::tr1::unordered_set<FragmentEventListener *> fel_set;
    typedef std::tr1::unordered_set<TabletEventListener *> tel_set;

private:
    inline Tablet * findContainingTablet(strref_t row) const;

private:
    TableSchema schema;
    size_t schemaVersion;
    CommitRing rowCommits;

    memfragvec_vec groupMemFrags;

    group_map groupIndex;

    tablet_vec tablets;

    fel_set fragmentListeners;
    tel_set tabletListeners;
};

//----------------------------------------------------------------------------
// TableLock
//----------------------------------------------------------------------------
class kdi::server::TableLock
    : private boost::noncopyable
{
public:
    explicit TableLock(Table const * table) :
        _lock(table->tableMutex) {}

    TableLock(Table const * table, bool doLock) :
        _lock(table->tableMutex, doLock) {}

    void lock() { _lock.lock(); }
    void unlock() { _lock.unlock(); }
    bool locked() const { return _lock.locked(); }

private:
    boost::mutex::scoped_lock _lock;
};


#endif // KDI_SERVER_TABLE_H
