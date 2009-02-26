#include <server_rpc.h>

typedef std::pair<char const *, char const *> PackedCells;
typedef boost::mutex::scoped_lock lock_t;

//----------------------------------------------------------------------------
// Synchronization
//----------------------------------------------------------------------------

// Lock levels: if holding multiple locks, they must always be
// acquired in level order:
//
//  - TabletServer::serverMutex
//  - TableInfo::tableMutex
//  - TableScanner::scannerMutex


//----------------------------------------------------------------------------
// CellBuffer
//----------------------------------------------------------------------------
class CellBuffer
{
    

public:
    bool checkCellOrder() const;

};

//----------------------------------------------------------------------------
// TableInfo
//----------------------------------------------------------------------------
class TableInfo
{
    boost::mutex tableMutex;
    std::string name;

public:
    bool checkForLoadedTablets(CellBufferPtr const & cells) const;
    bool checkRowCommits(CellBufferPtr const & cells, int64_t commitMaxTxn) const;
    void updateRowCommits(CellBufferPtr const & cells, int64_t commitTxn);

    /// Get the ordered chain of fragments to merge for the first part
    /// of the given predicate.  Returns true if such a chain is
    /// available (may be false if the tablet corresponding to the
    /// first part of the range is not loaded).  When the tablet is
    /// available, the fragment chain is returned in proper merge
    /// order.  The row interval over which the chain is valid is also
    /// returned.
    bool getFirstFragmentChain(
        ScanPredicate const & predicate,
        vector<Fragment const *> & fragmentChain,
        Interval<string> & rows) const;
};




//----------------------------------------------------------------------------
// TableScanner
//----------------------------------------------------------------------------
class TableScanner
{
    boost::mutex scannerMutex;
    
    TableInfo * table;
    ScanPredicate pred;

    FragmentMerge merge;
    int64_t scanTxn;
    Interval<string> rows;

    CellKey lastKey;

    struct ScanState
    {
        string lastRow;
        int64_t scanTxn;
        bool scanContinues;
        bool rowContinues;
    };


public:
    // Read more data from the scan.  Try to limit the output to
    // approximately maxCells or maxSize, whichever comes first.  The
    // function returns true on success or false if the scan refers to
    // an invalid range.  On success, the retrieved cells will be
    // stored in packedCells.  It's possible for packedCells to be
    // empty, even on a successful call.
    //
    // The incremental state of the scan is stored in scanState.  The state includes information necessary to restore a scan

    // The last row seen is returned in lastRow.  It may be
    // later than anything appearing in packedCells, but it will not
    // be earlier.  The lastRow represents the first row that would
    // need to be examined should the scanner have to be restarted.


    bool next(int64_t maxCells, int64_t maxSize,
              CellBlockBuilder & cells,
              ScanState & scanState)
    {
        lock_t scannerLock(scannerMutex);

        // Open first range:
        //   maxTxn = getLastCommitTxn()  (may be >> than getMaxTxn(frags))
        
        // Open subsequent range:
        //   lastTxn = getLastCommitTxn()
        //   if lastTxn > maxTxn and getMaxTxn(frags) > maxTxn:
        //      break  (don't want to mix newer results with old)

        // Return maxTxn

        
        bool firstRange = !merge;
        int64_t maxTxn = scanTxn;

        bool noTablet = false;
        for(;;)
        {
            // If merge is not active, set it up
            if(!merge)
            {
                pred = pred.clipRows(lastRow);
                if(pred.getRowPredicate()->isEmpty())
                    break;

                // Need table lock -- unlock scanner lock first
                scannerLock.unlock();
                lock_t tableLock(table->tableMutex);
                
                // Get the last commit transaction for the table
                scanTxn = table->getLastCommitTxn();

                // Get the next fragment chain
                vector<Fragment const *> chain;
                if(!table->getFirstFragmentChain(pred, chain, rows))
                {
                    // The next range isn't on this server
                    noTablet = true;
                    break;
                }

                // Register our interest in events for the new row
                // range
                table->addRowListener(rows, this);

                // Done with table lock, need scanner lock again.
                scannerLock.lock();
                tableLock.unlock();

                // Open the merge
                merge.reset(new FragmentMerge(chain));

                // If this is the first range we've scanned, set the
                // max txn for the batch
                if(firstRange)
                {
                    maxTxn = scanTxn;
                    firstRange = false;
                }
                // Else if the max transaction for the new chain is
                // greater than the transaction on the batch so far,
                // end the batch.  We don't want to mix old data with
                // new.
                else if(scanTxn > maxTxn && getMaxTxn(chain) > maxTxn)
                {
                    break;
                }
            }

            assert(merge);

            // We have an active merge, get some (more) data
            merge.next(maxCells - cells.getCellCount(),
                       maxSize - cells.getDataSize(),
                       cells,
                       state);

            

        }

    }

    int64_t getScanTxn() const;
    void getPackedCells(PackedCells & packedCells) const;
};


//----------------------------------------------------------------------------
// TabletServerI
//----------------------------------------------------------------------------
class TabletServerI : public TabletServer
{
    boost::mutex serverMutex;

private:
    CellBufferPtr decodeCells(PackedCells const & packedCells) const;
    TableScannerPtr allocateScanner(TableInfo * tableInfo, ScanPredicate const & predicate, ScanMode mode) const;

    TableInfo * getTable(string const & tableName) const;

    int64_t getLastCommitTxn() const;
    int64_t assignCommitTxn();

    size_t assignScannerId();

    void queueCommit(CellBufferPtr const & cells, int64_t commitTxn);

    void deferUntilDurable(ApplyCbPtr const & cb, int64_t commitTxn);
    void deferUntilDurable(SyncCbPtr const & cb, int64_t waitForTxn);

public:
    typedef AMD_TabletServer_applyPtr ApplyCbPtr;
    typedef AMD_TabletServer_syncPtr  SyncCbPtr;
    typedef AMD_TabletServer_scanPtr  ScanCbPtr;

    void apply_async(ApplyCbPtr const & cb,
                     string const & table,
                     PackedCells const & packedCells,
                     int64_t commitMaxTxn,
                     bool waitForSync,
                     Ice::Current const & cur)
    {
        // Decode cells
        CellBufferPtr cells = decodeCells(packedCells);
        if(!cells)
        {
            cb.ice_exception(InvalidPackedFormatError());
            return;
        }
        
        // Make sure cells are in order and unique
        if(!cells->checkCellOrder())
        {
            cb.ice_exception(CellDisorderError());
            return;
        }

        // Try to apply the commit
        lock_t lock(serverMutex);

        // Find the table
        TableInfo * tableInfo = getTable(table);
        if(!table)
        {
            lock.unlock();
            cb.ice_exception(TabletNotLoadedError());
            return;
        }

        // Note: if the tablets are assigned to this server but still
        // in the process of being loaded, may want to defer until
        // they are ready.
        
        // Make sure all cells map to loaded tablets
        if(!tableInfo->checkForLoadedTablets(cells))
        {
            lock.unlock();
            cb.ice_exception(TabletNotLoadedError());
            return;
        }

        // Make sure the last transaction in each row is less than or
        // equal to commitMaxTxn (which is trivially true if
        // commitMaxTxn >= last commit)
        if(commitMaxTxn < getLastCommitTxn() &&
           !tableInfo->checkRowCommits(cells, commitMaxTxn))
        {
            lock.unlock();
            cb.ice_exception(MutationConflictError());
            return;
        }

        // How to throttle?

        // We don't want to queue up too many heavy-weight objects
        // while waiting to commit to the log.  It would be bad to
        // make a full copy of the input data and queue it.  If we got
        // flooded with mutations we'd either: 1) use way too much
        // memory if we had a unbounded queue, or 2) block server
        // threads and starve readers if we have a bounded queue.
        
        // Ideally, we'd leave mutations "on the wire" until we were
        // ready for them, while allowing read requests to flow
        // independently.

        // If we use the Slice array mapping for the packed cell block
        // (and the backing buffer stays valid for the life of the AMD
        // invocation), then we can queue lightweight objects.  Ice
        // might be smart enough to do flow control for us in this
        // case.  I'm guessing it has a buffer per connection.  If we
        // tie up the buffer space, then the writing connection should
        // stall while allowing other connections to proceed.  Of
        // course interleaved reads on the same connection will
        // suffer, but that's not as big of a concern.

        // Throttle later...



        // Assign transaction number to commit
        int64_t txn = assignCommitTxn();
        
        // Update latest transaction number for all affected rows
        tableInfo->updateRowCommits(cells, txn);
        
        // Push the cells to the log queue
        queueCommit(cells, txn);

        lock.unlock();

        // Defer response if necessary
        if(waitForSync)
        {
            deferUntilDurable(txn, cb);
            return;
        }

        // Respond now
        cb->ice_response(txn);
        return;
    }
    
    void sync_async(SyncCbPtr const & cb,
                    int64_t waitForTxn,
                    Ice::Current const & cur)
    {
        lock_t lock(serverMutex);

        // Clip waitForTxn to the last committed transaction
        if(waitForTxn > getLastCommitTxn())
            waitForTxn = getLastCommitTxn();

        // Get the last durable transaction;
        int64_t syncTxn = getLastDurableTxn();
        
        lock.unlock();

        // Defer response if necessary
        if(waitForTxn > lastDurableTxn)
        {
            deferUntilDurable(waitForTxn, cb);
            return;
        }

        // Respond now
        cb->ice_response(syncTxn);
        return;
    }

    void scan_async(ScanCbPtr const & cb,
                    string const & table,
                    string const & predicate,
                    ScanMode mode,
                    ScanParams const & params,
                    Ice::Current const & cur)
    {
        // Verify the scan mode is one we support
        switch(mode)
        {
            case SCAN_ANY_TXN:
                break;

            default:
                cb->ice_exception(ScanModeError());
                return;
        }

        // Parse the scan predicate
        ScanPredicate pred;
        try {
            pred = ScanPredicate(predicate);
        }
        catch(...) {
            cb->ice_exception(InvalidPredicateError());
            return;
        }

        lock_t lock(serverMutex);

        // Get an ID for the scanner
        size_t scanId = assignScannerId();

        // Get the table for the scanner
        TableInfo * tableInfo = getTable(table);

        lock.unlock();

        if(!tableInfo)
        {
            cb->ice_exception(TabletNotLoadedError());
            return;
        }
        
        // Make a scanner object
        TableScannerPtr scanner = allocateScanner(tableInfo, predicate, mode);

        // Fetch the next result block
        ScanResult result;
        result.endOfScan = !scanner->advance(
            params.maxCells,
            params.maxSize,
            result.cells,
            result.scanTxn);
        
        // If the client wants the scanner closed, close it
        if(params.close)
            result.endOfScan = true;
        
        // If there's more to scan, set up a continuation object
        ScannerPrx scanContinuation;
        if(!result.endOfScan)
        {
            // Make identity
            Ice::Identity id;
            id.category = "scan";
            id.name = scanLocator->getNameFromId(scanId);

            // Make ICE object for scanner
            ScannerIPtr obj = new ScannerI(scanner, scanLocator);

            // Add it to the scanner locator
            scanLocator->add(scanId, obj);

            // Set the continuation proxy
            scanContinuation = ScannerPrx::uncheckedCast(
                cur.adapter->createProxy(id));
        }

        // Return the results
        cb->ice_response(result, scanContinuation);
        return;
    }
};

//----------------------------------------------------------------------------
// ScannerI
//----------------------------------------------------------------------------
class ScannerI : public Scanner
{
public:
    typedef AMD_Scanner_scanMorePtr  ScanMoreCbPtr;
    
    void scanMore_async(ScanMoreCbPtr const & cb,
                        ScanParams const & params,
                        Ice::Current const & cur)
    {
    }

    void close(Ice::Current const & cur)
    {
    }
};
