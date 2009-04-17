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

#ifndef KDI_SERVER_COMPACTOR_H
#define KDI_SERVER_COMPACTOR_H

#include <kdi/server/CellBuffer.h>
#include <kdi/server/DiskFragment.h>
#include <kdi/server/DiskOutput.h>
#include <kdi/server/FragmentEventListener.h>
#include <kdi/strref.h>
#include <boost/function.hpp>
#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>

namespace kdi {
namespace server {

    class BlockCache;
    class RangeFragmentMap;
    class Compactor;

} // namespace server
} // namespace kdi

class kdi::server::RangeFragmentMap 
{
public:
    typedef warp::Interval<std::string> range_t;

    // Range intervals cannot overlap so order by lower bound
    struct RangeLt 
    {
        bool operator()(range_t const & a, range_t const & b)
        {
            return a.getLowerBound() < b.getLowerBound();
        }
    };

    typedef std::vector<FragmentCPtr> frag_list_t;
    typedef std::map<range_t, frag_list_t, RangeLt> map_t;
    typedef map_t::const_iterator const_iterator;
    map_t rangeMap;

    void clear() { rangeMap.clear(); }
    RangeFragmentMap & operator=(RangeFragmentMap const & x);
    const_iterator begin() const { return rangeMap.begin(); }
    const_iterator end() const { return rangeMap.end(); }

    void addFragment(range_t range, FragmentCPtr const & frag);
};

class kdi::server::Compactor :
    public boost::noncopyable
{
    BlockCache * cache;
    DiskOutput writer;
    boost::scoped_ptr<boost::thread> thread;

public:

    /// Compact some ranges.  
    ///
    /// Input: The compaction set specifies a set of ranges
    /// from a single table and the fragments to compact for each of those 
    /// ranges.  Compaction proceeds from lowest range to highest and creates
    /// at least one new fragment containing the merged ranges.  If internal
    /// threshholds are reached, compaction may stop before all ranges in the 
    /// set have been compacted.
    ///
    /// Output: The output map contains only the ranges which were compacted.  If a
    /// fragment is given for the output range, that fragment replaces all
    /// the fragments given for that range in the compaction set.  If no
    /// fragment is specified for the output range, the fragments for that range
    /// had no output and can be removed from the ranges tablet set.
    void compact(RangeFragmentMap const & compactionSet, 
                 RangeFragmentMap & outputSet);
    
    /// Choose a more limited compaction set given a set of all the fragment
    /// sequences from a single table.
    void chooseCompactionSet(RangeFragmentMap const & fragMap,
                             RangeFragmentMap & compactionSet);

    void compactLoop();

    Compactor(BlockCache *cache);
    virtual ~Compactor() {}
};
#endif // KDI_SERVER_COMPACTOR_H
