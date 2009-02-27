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

namespace kdi {
namespace server {

    class Fragment;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// Fragment
//----------------------------------------------------------------------------
class Fragment
{
public:
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
    virtual FragmentBlock * loadBlock(size_t blockAddr) const = 0;
};

//----------------------------------------------------------------------------
// FragmentBlock
//----------------------------------------------------------------------------
class FragmentBlock
{
public:
    virtual void getFirstKey(CellKey & key) const = 0;
    virtual void getLastKey(CellKey & key) const = 0;

    virtual FragmentBlockReader * makeReader() const = 0;
};

//----------------------------------------------------------------------------
// FragmentBlockReader
//----------------------------------------------------------------------------
class FragmentBlockReader
{
public:
    virtual void skipTo(CellKey const & key) = 0;
    virtual void skipPast(CellKey const & key) = 0;

    
    virtual bool copyUntil(CellKey const & key, CellBuilder & out) = 0;
};


#endif // KDI_SERVER_FRAGMENT_H
