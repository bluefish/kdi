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

#include <kdi/server/TransactionCounter.h>
#include <ex/exception.h>
#include <vector>
#include <memory>

using namespace kdi;
using namespace kdi::server;
using namespace ex;

//----------------------------------------------------------------------------
// TransactionCounter::CallbackRunner
//----------------------------------------------------------------------------
class TransactionCounter::CallbackRunner
    : public warp::Runnable
{
    std::vector<txn_cb> callbacks;
    int64_t lastDurable;
    int64_t lastStable;
            
public:
    CallbackRunner(std::vector<txn_cb> const & callbacks,
                   int64_t lastDurable,
                   int64_t lastStable) :
        callbacks(callbacks),
        lastDurable(lastDurable),
        lastStable(lastStable)
    {
    }
            
    void run() throw()
    {
        for(std::vector<txn_cb>::const_iterator i = callbacks.begin();
            i != callbacks.end(); ++i)
        {
            i->second->done(i->first, lastDurable, lastStable);
        }
        delete this;
    }
};

//----------------------------------------------------------------------------
// TransactionCounter::CbHeap
//----------------------------------------------------------------------------
void TransactionCounter::CbHeap::deferUntil(TransactionCb * cb, int64_t waitTxn)
{
    assert(cb);
    if(waitTxn <= lastTxn)
    {
        raise<ValueError>("wait for past txn: wait=%d, last=%d",
                          waitTxn, lastTxn);
    }
    cbHeap.push(txn_cb(waitTxn, cb));
}

std::vector<TransactionCounter::txn_cb>
TransactionCounter::CbHeap::setLast(int64_t txn)
{
    std::vector<txn_cb> r;

    assert(txn > lastTxn);
    lastTxn = txn;

    while(!cbHeap.empty() && cbHeap.top().first <= lastTxn)
    {
        r.push_back(cbHeap.top());
        cbHeap.pop();
    }
    return r;
}

//----------------------------------------------------------------------------
// TransactionCounter
//----------------------------------------------------------------------------
TransactionCounter::TransactionCounter() :
    lastCommit(0), durable(0), stable(0)
{
}

warp::Runnable * TransactionCounter::setLastDurable(int64_t txn)
{
    std::vector<txn_cb> cbs = durable.setLast(txn);
    if(cbs.empty())
        return 0;
    return new CallbackRunner(cbs, durable.getLast(), stable.getLast());
}

warp::Runnable * TransactionCounter::setLastStable(int64_t txn)
{
    std::vector<txn_cb> cbs = stable.setLast(txn);
    if(cbs.empty())
        return 0;
    return new CallbackRunner(cbs, durable.getLast(), stable.getLast());
}
