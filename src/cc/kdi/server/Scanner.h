//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-02-26
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

#ifndef KDI_SERVER_SCANNER_H
#define KDI_SERVER_SCANNER_H

#include <kdi/server/FragmentEventListener.h>
#include <kdi/server/TabletEventListener.h>
#include <kdi/CellKey.h>
#include <kdi/scan_predicate.h>
#include <warp/string_range.h>
#include <warp/interval.h>
#include <boost/thread/mutex.hpp>
#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <exception>
#include <string>

namespace kdi {
namespace server {

    class Scanner;
    typedef boost::shared_ptr<Scanner> ScannerPtr;

    // Forward declaration
    class Table;
    class FragmentMerge;
    class BlockCache;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// Scanner
//----------------------------------------------------------------------------
class kdi::server::Scanner
    : public kdi::server::FragmentEventListener,
      public kdi::server::TabletEventListener,
      private boost::noncopyable
{
public:
    enum ScanMode {
        // Don't worry about read isolation, scan whatever's there.
        SCAN_ANY_TXN,

        // Make sure all cells in the row are from the same
        // transaction.  Ignore modifications that happen while the
        // scanner is mid-row.  ScanConflictErrors should only happen
        // if the server lacks sufficient resources to maintain
        // historical information.
        SCAN_ISOLATED_ROW_TXN,

        // Make sure all cells in the row are from the latest known
        // transaction.  If modifications happen while the scanner is
        // mid-row, fail with a ScanConflictError.
        SCAN_LATEST_ROW_TXN,
    };

public:
    class ScanCb
    {
    public:
        virtual void done() = 0;
        virtual void error(std::exception const & err) = 0;
    protected:
        ~ScanCb() {}
    };

public:
    Scanner(Table * table, BlockCache * cache,
            ScanPredicate const & pred, ScanMode mode);
    ~Scanner();

public:
    /// Read more data from the scan.  Try to limit the output to
    /// approximately maxCells or maxSize, whichever comes first.
    /// After the callback completes, the results will be available
    /// from the various scanner query methods.
    void scan_async(ScanCb * cb, size_t maxCells, size_t maxSize);

    /// Get the encoded cell block from the last scan.
    warp::StringRange getPackedCells() const;

    /// Get the last cell key seen by this scan.  This can be helpful
    /// in reporting progress or starting a new scan where this one
    /// left off.  Returns null if the scan has yet to return anything.
    CellKey const * getLastKey() const
    {
        return haveLastKey ? &lastKey : 0;
    }

    /// True if another call to scan could yield more results.
    bool scanContinues() const { return !endOfScan; }

    /// Get the transaction number for the most recent scan.
    int64_t getScanTransaction() const { return scanTxn; }


public: // FragmentEventListener

    virtual void onNewFragment(
        warp::Interval<std::string> const & r,
        Fragment const * f);

    virtual void onReplaceFragments(
        warp::Interval<std::string> const & r,
        std::vector<Fragment const *> const & f1,
        Fragment const * f2);


public: // TabletEventListener

    virtual void onTabletSplit(
        warp::Interval<std::string> const & lo,
        warp::Interval<std::string> const & hi);

    virtual void onTabletMerge(
        warp::Interval<std::string> const & lo,
        warp::Interval<std::string> const & hi);

    virtual void onTabletDrop(
        warp::Interval<std::string> const & r);

private:
    typedef boost::mutex::scoped_lock lock_t;
    class PackedOutput;

    bool startMerge(lock_t & scannerLock);

private: // mess in progress
    boost::mutex scannerMutex;

    Table * table;
    BlockCache * cache;
    ScanPredicate pred;

    boost::scoped_ptr<FragmentMerge> merge;
    int64_t scanTxn;
    warp::Interval<std::string> rows;

    boost::scoped_ptr<PackedOutput> output;
    CellKey lastKey;
    bool haveLastKey;
    
    bool endOfScan;
};


#endif // KDI_SERVER_SCANNER_H
