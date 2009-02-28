//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/server/FragmentMerge.h $
//
// Created 2009/02/26
//
// Copyright 2009 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_SERVER_FRAGMENTMERGE_H
#define KDI_SERVER_FRAGMENTMERGE_H

#include <kdi/scan_predicate.h>
#include <warp/heap.h>
#include <boost/scoped_array.hpp>

namespace kdi {
namespace server {

    class FragmentMerge;

    // Forward declarations
    class BlockCache;
    class Fragment;
    class CellBuilder;

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
    FragmentMerge(std::vector<Fragment const *> const & fragments,
                  BlockCache * cache,
                  ScanPredicate const & pred,
                  CellKey const * startAfter);
    ~FragmentMerge();

    bool copyMerged(size_t maxCells, size_t maxSize,
                    CellBuilder & cells);
};

#endif // KDI_SERVER_FRAGMENTMERGE_H
