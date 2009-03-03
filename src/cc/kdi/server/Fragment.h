//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/server/Fragment.h $
//
// Created 2009/02/26
//
// Copyright 2009 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_SERVER_FRAGMENT_H
#define KDI_SERVER_FRAGMENT_H

#include <warp/Iterator.h>
#include <warp/string_range.h>
#include <memory>
#include <stddef.h>

namespace kdi {
namespace server {

    class Fragment;
    class FragmentBlock;
    class FragmentBlockReader;

    typedef warp::Iterator<warp::StringRange> FragmentRowIterator;

    // Forward declarations
    class CellBuilder;

} // namespace server

    // Forward declarations
    class ScanPredicate;
    class CellKey;

} // namespace kdi

//----------------------------------------------------------------------------
// Fragment
//----------------------------------------------------------------------------
class kdi::server::Fragment
{
public:
    virtual ~Fragment() {}

    /// Get the address of the next block in this Fragment that could
    /// match the given predicate.  The minimum block to consider is
    /// given by minBlock.  This should be one past the last block
    /// returned by nextBlock, or zero for the first call.  When there
    /// are no more blocks that could match, this function will return
    /// size_t(-1).
    virtual size_t nextBlock(ScanPredicate const & pred,
                             size_t minBlock) const = 0;
    
    /// Allocate and load the block for this fragment at the given
    /// address.  The address must have been returned by nextBlock().
    virtual std::auto_ptr<FragmentBlock>
    loadBlock(size_t blockAddr) const = 0;

    /// Create an iterator over all rows in this Fragment.
    virtual std::auto_ptr<FragmentRowIterator>
    makeRowIterator() const = 0;
};

//----------------------------------------------------------------------------
// FragmentBlock
//----------------------------------------------------------------------------
class kdi::server::FragmentBlock
{
public:
    virtual ~FragmentBlock() {}

    /// Create a reader over the block.  The reader will only return
    /// cells matching the given predicate.
    virtual std::auto_ptr<FragmentBlockReader>
    makeReader(ScanPredicate const & pred) const = 0;
};

//----------------------------------------------------------------------------
// FragmentBlockReader
//----------------------------------------------------------------------------
class kdi::server::FragmentBlockReader
{
public:
    virtual ~FragmentBlockReader() {}

    /// Advance the reader to the next cell.  Returns true if there is
    /// another cell, false if the end of the block has been reached.
    /// The key for next cell, if any, is stored in nextKey.
    virtual bool advance(CellKey & nextKey) = 0;
    
    /// Copy from the reader into the output builder until the end of
    /// the block is reached or stopKey is found.  If stopKey is null,
    /// copy to the end of the block.  If the key is the stopping
    /// condition, it will not be included in the output.  Instead, it
    /// will be the next key for advance().  Each call to copyUntil()
    /// should be preceeded by a call to advance() to get the reader's
    /// starting position set up properly.  Calling copyUntil()
    /// multiple times without interleaved calls to advance() is
    /// undefined.
    virtual void copyUntil(CellKey const * stopKey, CellBuilder & out) = 0;
};


#endif // KDI_SERVER_FRAGMENT_H
