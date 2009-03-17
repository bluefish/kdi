//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-11
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

#include <kdi/server/CommitRing.h>
#include <ex/exception.h>

using namespace kdi;
using namespace kdi::server;
using namespace ex;


//----------------------------------------------------------------------------
// CommitRing
//----------------------------------------------------------------------------
CommitRing::CommitRing(int64_t startTxn, size_t purgeThreshold) :
    minTxn(startTxn),
    purgeThreshold(purgeThreshold),
    currentSize(0),
    pool(256u)
{
}

void CommitRing::setCommit(strref_t row, int64_t txn)
{
    if(txn < getMaxCommit())
        raise<ValueError>("decreasing txn: %d < %d", txn, getMaxCommit());

    index_t::const_iterator i = index.find(row);
    if(i != index.end())
    {
        ring.moveToFront(*i);
        (*i)->txn = txn;
        return;
    }

    Commit * x = pool.get();
    x->row.assign(row.begin(), row.end());
    x->txn = txn;

    ring.moveToFront(x);
    index.insert(x);

    currentSize += sizeof(Commit) + row.size();
    if(currentSize > purgeThreshold)
        purge();
}

void CommitRing::purge()
{
    size_t targetSz = purgeThreshold / 2;

    while(currentSize > targetSz)
    {
        Commit * x = &ring.back();

        currentSize -= sizeof(Commit) + x->row.size();
        minTxn = x->txn;

        index.erase(x);
        pool.release(x);
    }

    index.rehash();
}
