//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-02-26
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

#ifndef KDI_SERVER_FRAGMENTMERGE_H
#define KDI_SERVER_FRAGMENTMERGE_H

#include <kdi/scan_predicate.h>
#include <warp/heap.h>
#include <boost/scoped_array.hpp>
#include <boost/shared_ptr.hpp>

namespace kdi {
namespace server {

    class FragmentMerge;

    // Forward declarations
    class BlockCache;
    class Fragment;
    class CellOutput;
    typedef boost::shared_ptr<Fragment const> FragmentCPtr;

} // namespace server

    class CellKey;

} // namespace kdi

//----------------------------------------------------------------------------
// FragmentMerge
//----------------------------------------------------------------------------
class kdi::server::FragmentMerge
{
    class Input;
    
    struct InputLt
    {
        inline bool operator()(Input * a, Input * b) const;
    };

    BlockCache * cache;
    ScanPredicate pred;
    
    size_t nInputs;
    boost::scoped_array<Input> inputs;
    warp::MinHeap<Input *, InputLt> heap;

public:
    FragmentMerge(std::vector<FragmentCPtr> const & fragments,
                  BlockCache * cache,
                  ScanPredicate const & pred,
                  CellKey const * startAfter);
    ~FragmentMerge();

    bool copyMerged(size_t maxCells, size_t maxSize, CellOutput & out);
};

#endif // KDI_SERVER_FRAGMENTMERGE_H
