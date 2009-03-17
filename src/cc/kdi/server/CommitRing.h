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

#ifndef KDI_SERVER_COMMITRING_H
#define KDI_SERVER_COMMITRING_H

#include <kdi/strref.h>
#include <warp/circular.h>
#include <warp/objectpool.h>
#include <warp/hashtable.h>
#include <warp/hsieh_hash.h>
#include <warp/string_range.h>
#include <string>

namespace kdi {
namespace server {

    class CommitRing;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// CommitRing
//----------------------------------------------------------------------------
class kdi::server::CommitRing
    : private boost::noncopyable
{
    enum { DEFAULT_PURGE = 1<<20 };

public:
    explicit CommitRing(int64_t startTxn=0,
                        size_t purgeThreshold=DEFAULT_PURGE);

    /// Get the earliest commit that might still be in the ring, or
    /// the latest commit *not* in the ring.
    int64_t getMinCommit() const
    {
        return minTxn;
    }

    /// Get the latest commit in the ring.
    int64_t getMaxCommit() const
    {
        return ring.empty() ? minTxn : ring.front().txn;
    }

    /// Get the last commit for the given row.  If the row is still in
    /// the ring, return the actual commit number for the row.
    /// Otherwise, return getMinCommit().
    int64_t getCommit(strref_t row) const
    {
        index_t::const_iterator i = index.find(row);
        return (i == index.end() ? minTxn : (*i)->txn);
    }

    /// Set a new commit number for the given row.  Commits must set
    /// in globally non-decreasing order.
    void setCommit(strref_t row, int64_t txn);

private:
    /// Purge the commit ring until its size is half of the purge
    /// threshold.
    void purge();

private:
    struct Commit
        : public warp::Circular<Commit>
    {
        std::string row;
        int64_t txn;
    };

    struct CommitHash
    {
        static inline uint32_t hash(strref_t s) {
            return warp::hsieh_hash(s.begin(), s.size());
        }
        uint32_t operator()(Commit * x) const { return hash(x->row); }
        uint32_t operator()(strref_t x) const { return hash(x); }
    };

    struct CommitEq
    {
        bool operator()(Commit * a, strref_t b) const {
            return a->row == b;
        }
        bool operator()(strref_t a, Commit * b) const {
            return a == b->row;
        }
        bool operator()(Commit * a, Commit * b) const {
            return a->row == b->row;
        }
    };

    typedef warp::ObjectPool<Commit> pool_t;
    typedef warp::CircularList<Commit> ring_t;
    typedef warp::HashTable<Commit *, CommitHash, uint32_t, CommitEq> index_t;

private:
    int64_t minTxn;
    size_t const purgeThreshold;
    size_t currentSize;

    index_t index;
    pool_t pool;
    ring_t ring;
};

#endif // KDI_SERVER_COMMITRING_H
