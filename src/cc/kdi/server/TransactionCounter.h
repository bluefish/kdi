//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-10
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

#ifndef KDI_SERVER_TRANSACTIONCOUNTER_H
#define KDI_SERVER_TRANSACTIONCOUNTER_H

#include <warp/heap.h>
#include <warp/Runnable.h>
#include <utility>
#include <boost/noncopyable.hpp>

namespace kdi {
namespace server {

    class TransactionCounter;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// TransactionCounter
//----------------------------------------------------------------------------
class kdi::server::TransactionCounter
    : private boost::noncopyable
{
public:
    class TransactionCb
    {
    public:
        virtual void done(int64_t waitTxn,
                          int64_t lastDurableTxn,
                          int64_t lastStableTxn) = 0;
    protected:
        ~TransactionCb() {}
    };

public:
    TransactionCounter();

    /// Get the transaction number of the most recent commit.
    int64_t getLastCommit() const { return lastCommit; }

    /// Get the transaction number of the last durable commit.
    int64_t getLastDurable() const { return durable.getLast(); }

    /// Get the transaction number of the last stable commit.
    int64_t getLastStable() const { return stable.getLast(); }

    /// Assign a new commit transaction number.
    int64_t assignCommit() { return ++lastCommit; }
        
    /// Update the last transaction number that has been committed to
    /// permanent storage.  Calling this implies that all transactions
    /// earlier than the given number are also durable.  It should
    /// only be called with increasing transaction numbers.  Should
    /// the update allow some deferred callbacks to run, this function
    /// will return a Runnable object that will perform the callbacks.
    /// The object should be run() exactly once at the soonest
    /// convience.
    warp::Runnable * setLastDurable(int64_t txn);

    /// Update the last transaction number that has been serialized
    /// and saved in fragments.  Calling this implies that all
    /// transactions earlier than the given number are also stable.
    /// It should only be called with increasing transaction numbers.
    /// Should the update allow some deferred callbacks to run, this
    /// function will return a Runnable object that will perform the
    /// callbacks.  The object should be run() exactly once at the
    /// soonest convience.
    warp::Runnable * setLastStable(int64_t txn);

    /// Request a callback when the given transaction number has
    /// become durable.  The transaction must be after the current
    /// durable transaction.
    void deferUntilDurable(TransactionCb * cb, int64_t waitTxn)
    {
        durable.deferUntil(cb, waitTxn);
    }

    /// Request a callback when the given transaction number has
    /// become stable.  The transaction must be after the current
    /// stable transaction.
    void deferUntilStable(TransactionCb * cb, int64_t waitTxn)
    {
        stable.deferUntil(cb, waitTxn);
    }

private:
    typedef std::pair<int64_t, TransactionCb *> txn_cb;

    class CbHeap
    {
    public:
        explicit CbHeap(int64_t firstTxn) : lastTxn(firstTxn) {}
        int64_t getLast() const { return lastTxn; }
        void deferUntil(TransactionCb * cb, int64_t waitTxn);
        std::vector<txn_cb> setLast(int64_t txn);

    private:
        int64_t lastTxn;
        warp::MinHeap<txn_cb> cbHeap;
    };

private:
    int64_t lastCommit;

    CbHeap durable;
    CbHeap stable;

    class CallbackRunner;
};

#endif // KDI_SERVER_TRANSACTIONCOUNTER_H
