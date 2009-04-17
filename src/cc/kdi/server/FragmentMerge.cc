//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-02-27
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

#include <kdi/server/FragmentMerge.h>
#include <kdi/server/Fragment.h>
#include <kdi/server/BlockCache.h>
#include <kdi/server/CellOutput.h>
#include <kdi/CellKey.h>
#include <warp/util.h>
#include <boost/scoped_ptr.hpp>

using namespace kdi;
using namespace kdi::server;

//----------------------------------------------------------------------------
// FragmentMerge::Input
//----------------------------------------------------------------------------
class FragmentMerge::Input
{
    CellKey nextKey;
    size_t mergeIndex;

    Fragment const * fragment;
    size_t minBlock;

    FragmentBlock const * block;
    boost::scoped_ptr<FragmentBlockReader> reader;

private:
    /// Start reading the next block.  Returns true if there's a
    /// next block, false if we're out of blocks.
    bool startBlock(BlockCache * cache, ScanPredicate const & pred)
    {
        minBlock = fragment->nextBlock(pred, minBlock);
        if(minBlock == size_t(-1))
            return false;

        block = cache->getBlock(fragment, minBlock);
        reader.reset(block->makeReader(pred).release());
        return true;
    }

    /// Finish with the current block and release it to the cache.
    void finishBlock(BlockCache * cache)
    {
        cache->releaseBlock(block);
        block = 0;
        reader.reset();
        ++minBlock;
    }

public:
    /// Initialize input on the given fragment at the given merge
    /// index.
    void init(Fragment const * frag, size_t idx)
    {
        fragment = frag;
        mergeIndex = idx;
        minBlock = 0;
        block = 0;
    }

    /// Clean up input before destruction
    void cleanup(BlockCache * cache)
    {
        if(block)
            cache->releaseBlock(block);
    }

    /// Advance reader to the next key.  Returns true if the
    /// reader is ready to go and we have a valid next key.
    /// Returns false if we've exhausted the fragment.
    bool advance(BlockCache * cache, ScanPredicate const & pred)
    {
        for(;;)
        {
            if(!block && !startBlock(cache, pred))
                return false;
                
            assert(reader);
            if(reader->advance(nextKey))
                return true;

            finishBlock(cache);
        }
    }

    /// Copy from the current block into out until the end of the
    /// block is reached or stopKey is found.  If stopKey is null,
    /// copy to the end of the block.  If the key is the stopping
    /// condition, it will not be included in the output.  Return true
    /// if there is more data, false if the copy has exhausted the
    /// fragment.
    bool copyUntil(CellKey const * stopKey, ScanPredicate const & pred,
                   BlockCache * cache, CellOutput & out)
    {
        assert(reader);
        reader->copyUntil(stopKey, out);
        return advance(cache, pred);
    }
        
    /// Get the next key in this input stream.  Only meaningful if
    /// there's more data in this stream.
    CellKey const & getNextKey() const
    {
        return nextKey;
    }

    /// Order inputs
    bool operator<(Input & o) const
    {
        // Order first by nextKey (ascending)
        // Order second by mergeIndex (descending)
        return warp::order(
            nextKey, o.nextKey,
            o.mergeIndex, mergeIndex);
    }
};


//----------------------------------------------------------------------------
// FragmentMerge::InputLt
//----------------------------------------------------------------------------
bool FragmentMerge::InputLt::operator()(Input * a, Input * b) const
{
    return *a < *b;
}

//----------------------------------------------------------------------------
// FragmentMerge
//----------------------------------------------------------------------------
FragmentMerge::FragmentMerge(
    std::vector<Fragment const *> const & fragments,
    BlockCache * cache,
    ScanPredicate const & pred,
    CellKey const * startAfter) :
    cache(cache),
    pred(pred),
    nInputs(fragments.size())
{
    inputs.reset(new Input[nInputs]);
    for(size_t i = 0; i < nInputs; ++i)
    {
        Input * input = &inputs[i];
        input->init(fragments[i], i);
        bool more = input->advance(cache, pred);
        if(startAfter)
        {
            while(more && !(*startAfter < input->getNextKey()))
                more = input->advance(cache, pred);
        }
        if(more)
            heap.push(input);
    }
}

FragmentMerge::~FragmentMerge()
{
    for(size_t i = 0; i < nInputs; ++i)
        inputs[i].cleanup(cache);
}

bool FragmentMerge::copyMerged(size_t maxCells, size_t maxSize,
                               CellOutput & out)
{
    size_t const MAX_BLOCKS = 64;
    size_t nBlocks = 0;

    while(!heap.empty())
    {
        Input * top = heap.top();
        heap.pop();
        while(!heap.empty() &&
              !(top->getNextKey() < heap.top()->getNextKey()))
        {
            Input * o = heap.top();
            heap.pop();
            if(o->advance(cache, pred))
                heap.push(o);
        }

        bool hasMore = top->copyUntil(
            heap.empty() ? 0 : &heap.top()->getNextKey(),
            pred, cache, out);

        if(hasMore)
            heap.push(top);

        if(out.getCellCount() >= maxCells ||
           out.getDataSize() >= maxSize)
            break;

        if(++nBlocks >= MAX_BLOCKS)
            break;
    }

    return !heap.empty();
}
