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

#ifndef KDI_SERVER_FRAGMENT_H
#define KDI_SERVER_FRAGMENT_H

#include <warp/string_range.h>
#include <boost/shared_ptr.hpp>
#include <memory>
#include <stddef.h>

namespace kdi {
namespace server {

    class Fragment;
    class FragmentBlock;
    class FragmentBlockReader;

    typedef boost::shared_ptr<Fragment const> FragmentCPtr;

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
    virtual void copyUntil(CellKey const * stopKey, 
                           CellBuilder & out) = 0;
};


#endif // KDI_SERVER_FRAGMENT_H
