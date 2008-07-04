//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-08-23
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

#ifndef WARP_RANKCOLLECTOR_H
#define WARP_RANKCOLLECTOR_H

#include <ex/exception.h>
#include <vector>
#include <algorithm>

namespace warp
{
    /// A collection of the top K elements by rank, ranked with
    /// respect to a strict weak ordering relation.  Note that the top
    /// elements are determined by rank, not value.  For example,
    /// using the normal less-than ordering on integers, top ranked
    /// elements in a sorted list will be the smallest values.
    /// @param T Type of element.
    /// @param Lt Strict weak ordering on \c T.
    template <class T, class Lt=std::less<T> >
    class PartitionRankCollector
    {
        typedef std::vector<T> vec_t;

    public:
        typedef T value_t;
        typedef typename vec_t::iterator iterator;
        typedef typename vec_t::const_iterator const_iterator;
        typedef Lt value_compare_t;

    private:
        mutable vec_t buf;
        size_t k;
        size_t kCap;
        mutable size_t maxIdx;
        mutable bool stable;
        Lt lt;

        /// Trim the internal collection to the top K items.
        void trim() const
        {
            if(!stable)
            {
                if(buf.size() > k)
                {
                    iterator kth = buf.begin() + k;
                    std::nth_element(buf.begin(), kth, buf.end(), lt);
                    buf.erase(kth, buf.end());
                    maxIdx = k - 1;
                }
                stable = true;
            }
        }

    public:
        /// Keep a collection of the top K ranked elements seen so
        /// far in an iterated insertion sequence.
        /// @param k Number of elements to collect.
        /// @param lt Ordering relation.
        explicit PartitionRankCollector(size_t k, Lt const & lt = Lt()) :
            k(k), kCap(std::max(k*2, size_t(32))),
            maxIdx(0), stable(true), lt(lt)
        {
            using namespace ex;
            if(k == 0)
                raise<ValueError>("K must be positive");
            buf.reserve(kCap);
        }

        /// Keep a collection of the top K ranked elements seen so
        /// far in an iterated insertion sequence.
        /// @param k Number of elements to collect.
        /// @param kCap Number of elements to accumulate before reduction.
        /// @param lt Ordering relation.
        PartitionRankCollector(size_t k, size_t kCap, Lt const & lt = Lt()) :
            k(k), kCap(kCap), maxIdx(0), stable(true), lt(lt)
        {
            using namespace ex;
            if(k == 0)
                raise<ValueError>("K must be positive");
            if(kCap <= k)
                raise<ValueError>("capacity must be larger than K "
                                  "(K=%d)", k);
            buf.reserve(kCap);
        }

        /// Insert a new item into the collection.
        /// @return true if the item was accepted into the collection
        bool insert(T const & x)
        {
            if(buf.size() < k)
            {
                if(buf.empty() || lt(buf[maxIdx], x))
                    maxIdx = buf.size();
            }
            else if(!lt(x, buf[maxIdx]))
                return false;

            buf.push_back(x);
            stable = false;
            if(buf.size() == kCap)
                trim();

            return true;
        }

        /// Return true if the item would be rejected by a call to
        /// insert() -- e.g. insert() would return false.  This will
        /// only happen when the collection contains at least K items,
        /// and the given item is lower or equal rank than all of the
        /// items currently in the collection.
        bool wouldReject(T const & x) const
        {
            return (buf.size() >= k) && !lt(x, buf[maxIdx]);
        }

        /// Sort the top K items.
        void sort()
        {
            trim();
            std::sort(buf.begin(), buf.end(), lt);
            maxIdx = buf.size() - 1;
        }

        // Clear the collection.
        void clear()
        {
            buf.clear();
            stable = true;
            maxIdx = 0;
        }

        bool empty() const { return buf.empty(); }
        size_t size() const { return std::min(buf.size(), k); }

        iterator begin() { trim(); return buf.begin(); }
        iterator end() { trim(); return buf.end(); }

        const_iterator begin() const { trim(); return buf.begin(); }
        const_iterator end() const { trim(); return buf.begin(); }

        value_compare_t const & valueCompare() const { return lt; }
    };


    /// A collection of the top K elements by rank, ranked with
    /// respect to a strict weak ordering relation.  Note that the top
    /// elements are determined by rank, not value.  For example,
    /// using the normal less-than ordering on integers, top ranked
    /// elements in a sorted list will be the smallest values.
    /// @param T Type of element.
    /// @param Lt Strict weak ordering on \c T.
    template <class T, class Lt=std::less<T> >
    class HeapRankCollector
    {
        typedef std::vector<T> vec_t;

    public:
        typedef T value_t;
        typedef typename vec_t::iterator iterator;
        typedef typename vec_t::const_iterator const_iterator;
        typedef Lt value_compare_t;

    private:
        vec_t buf;
        size_t k;
        bool heapOrder;
        Lt lt;

    public:
        /// Keep a collection of the top K ranked elements seen so
        /// far in an iterated insertion sequence.
        /// @param k Number of elements to collect.
        /// @param lt Ordering relation.
        explicit HeapRankCollector(size_t k, Lt const & lt = Lt()) :
            k(k), heapOrder(true), lt(lt)
        {
            using namespace ex;
            if(k == 0)
                raise<ValueError>("K must be positive");
            buf.reserve(k);
        }

        /// Insert a new item into the collection.
        /// @return true if the item was accepted into the collection
        bool insert(T const & x)
        {
            if(!heapOrder)
            {
                std::make_heap(buf.begin(), buf.end(), lt);
                heapOrder = true;
            }
            
            if(buf.size() < k)
            {
                buf.push_back(x);
                std::push_heap(buf.begin(), buf.end(), lt);
                return true;
            }
            else if(lt(x, buf.front()))
            {
                std::pop_heap(buf.begin(), buf.end(), lt);
                buf.back() = x;
                std::push_heap(buf.begin(), buf.end(), lt);
                return true;
            }
            else
            {
                return false;
            }
        }

        /// Sort the top K items.
        void sort()
        {
            std::sort(buf.begin(), buf.end(), lt);
            heapOrder = false;
        }

        // Clear the collection.
        void clear()
        {
            buf.clear();
            heapOrder = true;
        }

        bool empty() const { return buf.empty(); }
        size_t size() const { return buf.size(); }

        iterator begin() { return buf.begin(); }
        iterator end() { return buf.end(); }

        const_iterator begin() const { return buf.begin(); }
        const_iterator end() const { return buf.begin(); }

        value_compare_t const & valueCompare() const { return lt; }
    };
}

#endif // WARP_RANKCOLLECTOR_H
