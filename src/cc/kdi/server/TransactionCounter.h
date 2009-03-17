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
    class DurableCb
    {
    public:
        virtual void done(int64_t waitTxn, int64_t lastDurableTxn) = 0;
    protected:
        ~DurableCb() {}
    };

public:
    TransactionCounter();

    /// Get the transaction number of the most recent commit.
    int64_t getLastCommit() const { return lastCommit; }

    /// Get the transaction number of the last durable commit.
    int64_t getLastDurable() const { return lastDurable; }

    /// Assign a new commit transaction number.
    int64_t assignCommit() { return ++lastCommit; }
        
    /// Update the last last transaction number that has been
    /// committed to permanent storage.  Calling this implies that all
    /// transactions earlier than the given number are also durable.
    /// It should only be called with increasing transaction numbers.
    /// Should the update allow some deferred callbacks to run, this
    /// function will return a Runnable object that will perform the
    /// callbacks.  The object should be run() exactly once at the
    /// soonest convience.
    warp::Runnable * setLastDurable(int64_t txn);

    /// Request a callback when the given transaction number has
    /// become durable.  The transaction must be after the current
    /// durable transaction.
    void deferUntilDurable(DurableCb * cb, int64_t waitTxn);

private:
    int64_t lastCommit;
    int64_t lastDurable;

    typedef std::pair<int64_t, DurableCb *> txn_cb;
    warp::MinHeap<txn_cb> cbHeap;

    class CallbackRunner;
};


#endif // KDI_SERVER_TRANSACTIONCOUNTER_H
