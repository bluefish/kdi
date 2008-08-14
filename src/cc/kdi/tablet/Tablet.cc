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

#include <kdi/tablet/Tablet.h>
#include <kdi/tablet/Fragment.h>
#include <kdi/tablet/SuperTablet.h>
#include <kdi/tablet/TabletConfig.h>
#include <kdi/tablet/Scanner.h>
#include <kdi/tablet/ConfigManager.h>
#include <kdi/tablet/FileTracker.h>
#include <kdi/tablet/SharedCompactor.h>
#include <kdi/tablet/SharedLogger.h>
#include <kdi/scan_predicate.h>
#include <kdi/cell_filter.h>
#include <kdi/cell_merge.h>
#include <warp/config.h>
#include <warp/functional.h>
#include <warp/algorithm.h>
#include <warp/fs.h>
#include <warp/uri.h>
#include <warp/log.h>
#include <ex/exception.h>
#include <boost/format.hpp>

#include <kdi/local/disk_table_writer.h>
using kdi::local::DiskTableWriter;

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace ex;
using namespace std;
using boost::format;

namespace {
    
    /// Split tablets that get larger than 400 MB
    size_t const SPLIT_THRESHOLD = 400 << 20;

    /// Make a printable name for the Tablet
    std::string makePrettyName(std::string const & tableName,
                               IntervalPoint<string> const & last)
    {
        ostringstream oss;
        oss << tableName << '(';
        if(last.isFinite())
            oss << reprString(last.getValue());
        else
            oss << "END";
        oss << ')';
        return oss.str();
    }

}

//----------------------------------------------------------------------------
// Tablet inline
//----------------------------------------------------------------------------
void Tablet::validateRow(strref_t row) const
{
    lock_t lock(mutex);
    if(!rows.contains(row, warp::less()))
        raise<RowNotInTabletError>("%s", row);
}

void Tablet::validateRows(ScanPredicate const & pred) const
{
    lock_t lock(mutex);
    if(pred.getRowPredicate())
    {
        if(!rows.contains(pred.getRowPredicate()->getHull()))
            raise<RowNotInTabletError>("%s", *pred.getRowPredicate());
    }
    else if(!rows.isInfinite())
        raise<RowNotInTabletError>("<< >>");
}


//----------------------------------------------------------------------------
// Tablet
//----------------------------------------------------------------------------
Tablet::Tablet(std::string const & tableName,
               ConfigManagerPtr const & configMgr,
               SharedLoggerPtr const & logger,
               SharedCompactorPtr const & compactor,
               FileTrackerPtr const & tracker,
               TabletConfig const & cfg,
               SuperTablet * superTablet) :
    configMgr(configMgr),
    logger(logger),
    compactor(compactor),
    tracker(tracker),
    superTablet(superTablet),
    tableName(tableName),
    server(cfg.getServer()),
    prettyName(makePrettyName(tableName, cfg.getTabletRows().getUpperBound())),
    rows(cfg.getTabletRows()),
    mutationsPending(false),
    configChanged(false),
    splitPending(false)
{
    log("Tablet %p %s: created", this, getPrettyName());

    EX_CHECK_NULL(configMgr);
    EX_CHECK_NULL(logger);
    EX_CHECK_NULL(compactor);
    EX_CHECK_NULL(tracker);

    // Load our fragments
    bool uriChanged = false;
    vector<string> const & uris = cfg.getTableUris();
    for(vector<string>::const_iterator i = uris.begin();
        i != uris.end(); ++i)
    {
        // Load the fragment from the config manager
        FragmentPtr frag = configMgr->openFragment(*i);
        fragments.push_back(frag);

        // Check to see if the fragment URI changed on load.
        if(fragments.back()->getFragmentUri() != *i)
            uriChanged = true;
    }

    // If a table URI changed during the load, resave our config
    if(uriChanged)
    {
        log("Tablet config changed on load, saving: %s", getPrettyName());
        lock_t lock(mutex);
        postConfigChange(lock);
        saveConfig(lock);
    }
}

Tablet::~Tablet()
{
    // Untrack files that we're still referencing -- once a Tablet is
    // destroyed, another process may load it.  We have to assume the
    // file names have been exported.
    for(fragments_t::const_iterator i = fragments.begin();
        i != fragments.end(); ++i)
    {
        tracker->untrack(
            uriPopScheme(
                (*i)->getFragmentUri()
                )
            );
    }    

    log("Tablet %p %s: destroyed", this, getPrettyName());
}

void Tablet::set(strref_t row, strref_t column, int64_t timestamp, strref_t value)
{
    validateRow(row);
    logger->set(shared_from_this(), row, column, timestamp, value);
    
    lock_t lock(mutex);
    mutationsPending = true;
}

void Tablet::erase(strref_t row, strref_t column, int64_t timestamp)
{
    validateRow(row);
    logger->erase(shared_from_this(), row, column, timestamp);

    lock_t lock(mutex);
    mutationsPending = true;
}
    
void Tablet::sync()
{
    lock_t lock(mutex);

    // Sync pending mutations
    while(mutationsPending)
    {
        mutationsPending = false;
        lock.unlock();
        logger->sync();
        lock.lock();
    }

    // Save our config
    saveConfig(lock);
}

CellStreamPtr Tablet::scan() const
{
    return scan(ScanPredicate());
}

CellStreamPtr Tablet::scan(ScanPredicate const & pred) const
{
    // Make sure this is a valid scan for this tablet
    validateRows(pred);

    // Strip the history predicate -- we'll implement it as a
    // filter after everything else
    ScanPredicate p(pred);
    int history = p.getMaxHistory();
    p.setMaxHistory(0);

    // Create a new scanner and keep track of it
    ScannerPtr scanner(new Scanner(shared_from_this(), p));
    LockedPtr<scanner_vec_t>(syncScanners)->push_back(scanner);

    // Add the history filter if we need it and return
    if(history)
    {
        CellStreamPtr filter = makeHistoryFilter(history);
        filter->pipeFrom(scanner);
        return filter;
    }
    else
        return scanner;
}

CellStreamPtr Tablet::getMergedScan(ScanPredicate const & pred) const
{
    validateRows(pred);

    // We don't support history predicates
    if(pred.getMaxHistory())
        raise<ValueError>("unsupported history predicate: %s", pred);

    // Get exclusive access to fragment list
    lock_t lock(mutex);

    // If there is only one table, no merge is necessary but we still
    // have to filter erasures.  The one exception to this is if the
    // single table is fully-compacted disk table.
    if(fragments.size() == 1)
    {
        if(fragments.front()->isImmutable())
        {
            // This is a compacted disk table.  It should contain no
            // erasures.
            return fragments.front()->scan(pred);
        }
        else
        {
            // The table may contain erasures.
            CellStreamPtr filter = makeErasureFilter();
            filter->pipeFrom(fragments.front()->scan(pred));
            return filter;
        }
    }

    // XXX may need to block if the merge width is too great

    // Need to build a merge stream (note that a 0-way merge is
    // possible and well-formed).  Tables added to the merge in
    // reverse order so later streams override earlier streams.
    CellStreamPtr merge = CellMerge::make(true);
    for(fragments_t::const_reverse_iterator i = fragments.rbegin();
        i != fragments.rend(); ++i)
    {
        merge->pipeFrom((*i)->scan(pred));
    }
    return merge;
}

TabletPtr Tablet::splitTablet()
{
    log("Tablet split: %s", getPrettyName());

    // Make sure outstanding mutations are stable
    sync();

    lock_t lock(mutex);

    // Choose a median row
    std::string splitRow = chooseSplitRow(lock);

    // Make the new intervals
    Interval<string> low(rows);
    Interval<string> high(rows);
    low.setUpperBound(splitRow, BT_INCLUSIVE);
    high.setLowerBound(splitRow, BT_EXCLUSIVE);

    // Make sure the split actually divides the tablet into two
    // non-empty parts.  Otherwise, return null.  Split is not
    // possible.
    if(low.isEmpty() || high.isEmpty())
    {
        log("No split possible: %s", getPrettyName());
        return TabletPtr();
    }

    // Save new tablet configs -- write the high first because it is
    // somewhat less expensive to recover from a missing low tablet
    // than a missing high tablet.
    vector<string> uris = getFragmentUris(lock);
    TabletConfig highCfg(high, uris, server);
    TabletConfig lowCfg(low, uris, server);
    configMgr->setTabletConfig(tableName, highCfg); // XXX should unlock
    configMgr->setTabletConfig(tableName, lowCfg);

    // Clone this tablet with new row bounds and change bounds on this
    // tablet
    TabletPtr lowTablet(new Tablet(*this, low));
    this->rows = high;

    log("Tablet split complete: low=%s high=%s",
        lowTablet->getPrettyName(),
        this->getPrettyName());

    // Release split flag
    splitPending = false;

    // Return new tablet
    return lowTablet;
}

Tablet::Tablet(Tablet const & o, Interval<string> const & rows) :
    configMgr(o.configMgr),
    logger(o.logger),
    compactor(o.compactor),
    tracker(o.tracker),
    superTablet(o.superTablet),
    tableName(o.tableName),
    server(o.server),
    prettyName(makePrettyName(tableName, rows.getUpperBound())),
    rows(rows),
    fragments(o.fragments),
    mutationsPending(false),
    configChanged(false),
    splitPending(false)
{
    // Add references to cloned fragment files
    for(fragments_t::const_iterator i = this->fragments.begin();
        i != this->fragments.end(); ++i)
    {
        tracker->addReference(
            uriPopScheme(
                (*i)->getFragmentUri()
                )
            );
    }
}

void Tablet::postConfigChange(lock_t const & lock)
{
    if(!lock)
        raise<ValueError>("need lock");
    
    configChanged = true;
}

void Tablet::saveConfig(lock_t & lock)
{
    if(!lock)
        raise<ValueError>("need lock");

    if(!configChanged)
        return;

    do
    {
        configChanged = false;
        vector<string> uris = getFragmentUris(lock);

        lock.unlock();

        // Save our config
        configMgr->setTabletConfig(tableName, TabletConfig(rows, uris, server));

        lock.lock();

        // Loop if something has updated the config while we were
        // saving the old one
    } while(configChanged);

    // Release dead files
    for(vector<string>::const_iterator i = deadFiles.begin();
        i != deadFiles.end(); ++i)
    {
        tracker->release(*i);
    }
    deadFiles.clear();
}

std::vector<std::string> Tablet::getFragmentUris(lock_t const & lock) const
{
    if(!lock)
        raise<ValueError>("need lock");

    // Build list of fragment uris
    vector<string> uris;
    uris.reserve(fragments.size());
    for(fragments_t::const_iterator i = fragments.begin();
        i != fragments.end(); ++i)
    {
        uris.push_back((*i)->getFragmentUri());
    }

    return uris;
}

void Tablet::addFragment(FragmentPtr const & fragment)
{
    EX_CHECK_NULL(fragment);

    log("Tablet %s: add fragment %s", getPrettyName(), fragment->getFragmentUri());

    bool wantCompaction = false;
    {
        // Lock tables
        lock_t lock(mutex);

        // Add to table list
        fragments.push_back(fragment);

        // Let's just say we want a compaction if we have more than 5
        // tables (or more than 4 static tables)
        wantCompaction = (fragments.size() > 5);

        // Save changes to config
        postConfigChange(lock);
    }

    // Request a compaction if we need one
    if(wantCompaction)
        compactor->requestCompaction(shared_from_this());
}

void Tablet::replaceFragments(std::vector<FragmentPtr> const & oldFragments,
                              FragmentPtr const & newFragment)
{
    // Check args
    if(oldFragments.empty())
        raise<ValueError>("replaceFragments with empty fragment sequence");
    EX_CHECK_NULL(newFragment);

    log("Tablet %s: replace %d fragments with %s", getPrettyName(),
        oldFragments.size(), newFragment->getFragmentUri());

    bool wantSplit = false;
    {
        // Lock tableSet
        lock_t lock(mutex);

        // Find old sequence in fragment vector
        fragments_t::iterator i = std::search(
            fragments.begin(), fragments.end(), oldFragments.begin(), oldFragments.end());
        if(i != fragments.end())
        {
            // Mark first fragment file for release
            deadFiles.push_back(
                uriPopScheme(
                    (*i)->getFragmentUri()
                    )
                );

            // Replace first item in the sequence
            *i = newFragment;

            // Mark rest of the old fragments for release
            fragments_t::iterator beginErase = ++i;
            for(size_t n = 1; n < oldFragments.size(); ++n, ++i)
            {
                deadFiles.push_back(
                    uriPopScheme(
                        (*i)->getFragmentUri()
                        )
                    );
            }
            
            // Remove the rest of the fragments from the set
            fragments.erase(beginErase, i);
        }
        else
        {
            // We didn't know about that sequence
            raise<RuntimeError>("replaceFragments with unknown fragment sequence");
        }

        // Save changes to config
        postConfigChange(lock);

        // Check to see if we should split
        if(!splitPending && superTablet &&
           getDiskSize(lock) >= SPLIT_THRESHOLD)
        {
            splitPending = true;
            wantSplit = true;
        }
    }

    // Update scanners
    updateScanners();

    // Maybe request a split
    if(wantSplit)
        superTablet->requestSplit(this);
}

namespace
{
    struct ReopenScannerOrRemove
    {
        /// Reopen the scanner if the handle is still valid.  If not,
        /// return true, as it should be removed from the list.
        bool operator()(ScannerWeakPtr const & weakScanner) const
        {
            ScannerPtr scanner = weakScanner.lock();
            if(scanner)
            {
                scanner->reopen();
                return false;   // don't remove scanner
            }
            else
                return true;    // remove expired scanner
        }
    };
}

size_t Tablet::getCompactionPriority() const
{
    lock_t lock(mutex);

    // Get the number of static tables in the set
    size_t n = fragments.size();
    if(n && !fragments.back()->isImmutable())
        --n;
    
    // Return the number of static tables we have (don't bother if
    // there's only one)
    return (n > 1 ? n : 0);
}

void Tablet::doCompaction()
{
    vector<FragmentPtr> compactionFragments;
    bool filterErasures = false;

    log("Tablet %s: start compaction", getPrettyName());
    
    // Choose the sequence of tables to compact
    {
        // Just compact the last K static tables -- maybe do something
        // fancy later

        int const K = 8;

        lock_t lock(mutex);
       
        // Find the last static table
        fragments_t::const_iterator last = fragments.begin();
        for(fragments_t::const_reverse_iterator i = fragments.rbegin(); i != fragments.rend(); ++i)
        {
            if((*i)->isImmutable())
            {
                last = i.base();
                break;
            }
        }
        // Get the beginning of the trailing range.  We want to filter
        // erasures if our compaction includes the first table.
        fragments_t::const_iterator first = fragments.begin();
        if(distance(first, last) > K)
            advance(first, distance(first, last) - K);
        else
            filterErasures = true;

        // Make a copy of the fragment pointers to be compacted
        compactionFragments.insert(compactionFragments.end(), first, last);
    }

    // Get an output file
    std::string fn = configMgr->getDataFile(tableName);
            
    // Open a DiskTable for writing
    DiskTableWriter writer(fn, 64<<10);

    // Build compaction merge stream
    CellStreamPtr merge = CellMerge::make(filterErasures);
    for(vector<FragmentPtr>::const_reverse_iterator i =
            compactionFragments.rbegin();
        i != compactionFragments.rend(); ++i)
    {
        merge->pipeFrom((*i)->scan(ScanPredicate()));
    }

    // Compact the tables
    Cell x;
    while(merge->get(x))
        writer.put(x);
    writer.close();     // XXX should sync file

    log("Tablet %s: end compaction", getPrettyName());

    // Reopen the table for reading
    std::string diskUri = uriPushScheme(fn, "disk");
    FragmentPtr frag = configMgr->openFragment(diskUri);

    // Track the new file for automatic deletion
    tracker->track(fn);

    // Update table set
    replaceFragments(compactionFragments, frag);
}

size_t Tablet::getDiskSize(lock_t const & lock) const
{
    if(!lock)
        raise<ValueError>("need lock");

    size_t sum = 0;
    for(fragments_t::const_iterator i = fragments.begin();
        i != fragments.end(); ++i)
    {
        /// XXX: need to know disk size of each fragment
        /// XXX: this is expensive, could cache this info
        sum += (*i)->getDiskSize(rows);
    }

    log("Tablet %s: diskSize = %s",
        getPrettyName(), sizeString(sum));

    return sum;
}

std::string Tablet::chooseSplitRow(lock_t & lock) const
{
    if(!lock)
        raise<ValueError>("need lock");

    typedef pair<string,size_t> pair_t;
    typedef flux::Stream<pair_t>::handle_t stream_t;
    
    size_t totalSize = getDiskSize(lock);
    stream_t merge = flux::makeMerge<pair_t>(warp::less());
    for(fragments_t::const_iterator i = fragments.begin();
        i != fragments.end(); ++i)
    {
        merge->pipeFrom((*i)->scanIndex(rows));
    }

    lock.unlock();

    size_t middle = totalSize / 2;
    size_t cumulativeSize = 0;
    pair_t x;
    while(cumulativeSize < middle && merge->get(x))
        cumulativeSize += x.second;
    
    log("Tablet %s: totalSize=%s, split=%.1f%%, splitRow=%s",
        getPrettyName(),
        sizeString(totalSize),
        100.0*cumulativeSize/totalSize,
        reprString(x.first));

    lock.lock();
    return x.first;
}

void Tablet::updateScanners() const
{
    LockedPtr<scanner_vec_t> scanners(syncScanners);

    // Reopen all valid scanners, remove all null scanners.
    scanners->erase(
        std::remove_if(
            scanners->begin(),
            scanners->end(),
            ReopenScannerOrRemove()),
        scanners->end());
}
