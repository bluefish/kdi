//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-02-25
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------


module kdi {
module rpc {

//----------------------------------------------------------------------------
// Parameter types
//----------------------------------------------------------------------------
sequence<byte> PackedCells;

const long kMaxTxn = 9223372036854775807;

struct ScanParams
{
    int maxCells;    // optional, approx cell limit for next batch
    int maxSize;     // optional, approx byte limit for next batch
    bool close;      // close scanner after next batch
};

struct ScanResult
{
    long scanTxn;       // server txn as of this batch
    bool scanComplete;  // last batch in scan; implies scanClosed
    bool scanClosed;    // scanner was closed on server
};


//----------------------------------------------------------------------------
// Errors
//----------------------------------------------------------------------------
exception TabletNotLoadedError {};

exception ScanModeError {};

exception InvalidPredicateError {};

exception ScannerBusyError {};


exception InvalidCellsError {};

exception InvalidPackedFormatError
    extends InvalidCellsError {};

exception CellDisorderError
    extends InvalidCellsError {};


exception TransactionError {};

exception MutationConflictError
    extends TransactionError {};

exception ScanConflictError
    extends TransactionError {};


//----------------------------------------------------------------------------
// ScanMode
//----------------------------------------------------------------------------
enum ScanMode {

    // Don't worry about read isolation, scan whatever's there.
    kScanAnyTxn,

    // Make sure all cells in the row are from the same transaction.
    // Ignore modifications that happen while the scanner is mid-row.
    // ScanConflictErrors should only happen if the server lacks
    // sufficient resources to maintain historical information.
    kScanIsolatedRowTxn,

    // Make sure all cells in the row are from the latest known
    // transaction.  If modifications happen while the scanner is
    // mid-row, fail with a ScanConflictError.
    kScanLatestRowTxn,
};


//----------------------------------------------------------------------------
// Scanner
//----------------------------------------------------------------------------
["ami","amd"]
interface Scanner
{
    /// Continue a previously opened scan.  All tablets necessary to
    /// fill the next result block must be currently loaded on this
    /// server.  If the scanner's transaction consistency requirements
    /// cannot be met, this function may fail with a
    /// ScanConflictError.  If the result indicates that the endOfScan
    /// has been reached, the scanner is automatically closed on the
    /// server.
    void scanMore(ScanParams params,
                  out ["cpp:array"]
                  PackedCells cells,
                  out ScanResult result)
        throws TabletNotLoadedError,
               ScanConflictError,
               ScannerBusyError;

    /// Close an in-progress scan.  This will free up any server
    /// resources associated with the scanner.  This call isn't
    /// necessary if a previous call to scanMore() reached the end of
    /// the scan, but it can be used to abort a scan before reaching
    /// the end.
    void close()
        throws ScannerBusyError;
};


//----------------------------------------------------------------------------
// TabletServer
//----------------------------------------------------------------------------
["ami","amd"]
interface TabletServer
{
    /// Apply a block of cells to the named table.  Cells must be
    /// sorted and unique, and the containing tablet for each cells
    /// must be currently loaded on this server.  The entire cell
    /// buffer will be applied in an atomic operation iff the latest
    /// server transaction for each affected row is less than or equal
    /// to commitMaxTxn.  Otherwise the call will fail with a
    /// MutationConflictError.  If waitForSync is true, the client
    /// will not get a response until the transaction has been made
    /// durable (in such case, no subsequent call to sync is
    /// necessary).  The server returns the transaction number
    /// assigned to the operation in commitTxn.
    void apply(string table,
               ["cpp:array"]
               PackedCells cells,
               long commitMaxTxn, // kMaxTxn indicates "don't care"
               bool waitForSync,  // act as if sync(commitTxn, -) called
               out long commitTxn)
        throws InvalidCellsError,
               TabletNotLoadedError,
               MutationConflictError;
    
    /// Wait until all transactions on the server have been made
    /// durable, up to and including waitForTxn.  If waitForTxn refers
    /// to some unassigned future transaction number, it will be
    /// clipped to the last known transaction.  When complete, the
    /// server returns the last durable transaction in syncTxn.
    void sync(long waitForTxn,  // kMaxTxn means "sync latest"
              out long syncTxn);

    /// Begin a scan on the named table, using the given predicate.
    /// All tablets necessary to fill the first result block must be
    /// currently loaded on this server.  The mode parameter is used
    /// to determine how transaction scan consistency issues should be
    /// handled.  The first block of scan is read and returned in the
    /// result parameter.  If the scan continues past a single result
    /// block, a proxy to a server-side continuation Scanner will be
    /// passed back in the scanner parameter.
    void scan(string table,
              string predicate,
              ScanMode mode,
              ScanParams params,
              out ["cpp:array"]
              PackedCells cells,
              out ScanResult result,
              out Scanner * continuedScanner)
        throws TabletNotLoadedError,
               InvalidPredicateError,
               ScanModeError;
};

}; // module rpc
}; // module kdi
