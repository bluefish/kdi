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
    int64_t lastDurable;
    std::vector<txn_cb> callbacks;
            
public:
    explicit CallbackRunner(int64_t lastDurable) :
        lastDurable(lastDurable) {}
            
    void run() throw()
    {
        for(std::vector<txn_cb>::const_iterator i = callbacks.begin();
            i != callbacks.end(); ++i)
        {
            i->second->done(i->first, lastDurable);
        }
        delete this;
    }

    void add(txn_cb const & x)
    {
        callbacks.push_back(x);
    }
};


//----------------------------------------------------------------------------
// TransactionCounter
//----------------------------------------------------------------------------
TransactionCounter::TransactionCounter() :
    lastCommit(0), lastDurable(0)
{
}

warp::Runnable * TransactionCounter::setLastDurable(int64_t txn)
{
    if(txn > lastDurable)
        lastDurable = txn;

    if(cbHeap.empty() || cbHeap.top().first > lastDurable)
        return 0;

    std::auto_ptr<CallbackRunner> r(new CallbackRunner(lastDurable));
    while(!cbHeap.empty() && cbHeap.top().first <= lastDurable)
    {
        r->add(cbHeap.top());
        cbHeap.pop();
    }
    return r.release();
}
        
void TransactionCounter::deferUntilDurable(DurableCb * cb, int64_t waitTxn)
{
    EX_CHECK_NULL(cb);
    if(waitTxn <= lastDurable)
        raise<ValueError>("wait for past txn: wait=%d, durable=%d",
                          waitTxn, lastDurable);

    cbHeap.push(txn_cb(waitTxn, cb));
}

