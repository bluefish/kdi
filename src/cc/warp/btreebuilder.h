//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/btreebuilder.h#1 $
//
// Created 2006/04/05
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_BTREEBUILDER_H
#define WARP_BTREEBUILDER_H

#include "btree.h"
#include "builder.h"
#include "util.h"
#include "file.h"

#include "ex/exception.h"

#include <vector>
#include <algorithm>

namespace warp
{
    template <class K, class P=uint64_t>
    class BTreeNodeBuilder;

    template <class K, class P=uint64_t>
    class BTreeBuilder;
}


//----------------------------------------------------------------------------
// BTreeNodeBuilder
//----------------------------------------------------------------------------
template <class K, class P>
class warp::BTreeNodeBuilder
{
public:
    typedef BTreeItem<K,P> item_t;
    typedef typename item_t::key_t key_t;
    typedef typename item_t::pos_t pos_t;

private:
    Builder builder;
    BuilderBlock * keyBlock;
    std::vector<char> outBuf;

    key_t firstKey;
    
    int nKeys;
    int maxKeys;

public:
    BTreeNodeBuilder(int maxKeys) :
        keyBlock(0),
        nKeys(-1),
        maxKeys(maxKeys)
    {
        reset();
    }

    /// Add the given (key,pos) pair to this node.
    void addKey(key_t const & key, pos_t pos)
    {
        if(++nKeys > 0)
        {
            // Not first key.  Write key and offset.
            keyBlock->append(item_t(key,pos));
        }
        else
        {
            // First key for this block.  Write offset, hold on to key.
            firstKey = key;
            builder << pos;
        }
    }

    /// Finalize and emit this node to an output file.
    void emit(FilePtr const & fp)
    {
        assert(fp);
        assert(!empty());

        if(!builder.isFinalized())
        {
            // Write keys array
            builder << *keyBlock << uint32_t(nKeys);

            // Finalize
            builder.finalize();

            // Copy to output buffer
            outBuf.resize(builder.getFinalSize());
            builder.exportTo(&outBuf[0]);
        }

        // Write to file
        size_t sz = fp->write(&outBuf[0], outBuf.size());
        if(sz != outBuf.size())
        {
            ex::raise<ex::IOError>
                ("BTreeNodeBuilder emitted %1% of %2% bytes",
                 sz, outBuf.size());
        }
    }

    /// Clear and reset for building a new node.
    void reset()
    {
        // Clear out, reset
        builder.reset();
        keyBlock = builder.subblock(8);
        nKeys = -1;
    }

    /// Get the first key sent to this block.  This key is not written
    /// to disk with this node.
    key_t const & getFirstKey() const { return firstKey; }

    /// Return true iff this node has at least one key.  Note that a
    /// node with only a first key is still considered empty.
    bool empty() const { return nKeys <= 0; }

    /// Return true iff this node has reached the maximum number of
    /// allowed keys.
    bool full() const { return nKeys >= maxKeys; }
};


//----------------------------------------------------------------------------
// BTreeBuilder
//----------------------------------------------------------------------------
template <class K, class P>
class warp::BTreeBuilder
{
public:
    typedef BTreeNodeBuilder<K,P>    builder_t;
    typedef BTreeTrailer<P>          trailer_t;
    typedef BTreeNode<K,P>           node_t;
    typedef typename node_t::item_t  item_t;
    typedef typename node_t::key_t   key_t;
    typedef typename node_t::pos_t   pos_t;

private:
    std::vector<builder_t *> levels;
    trailer_t trailer;
    FilePtr out;

    key_t lastKey;
    pos_t lastPos;

    pos_t minSeparation;
    int maxKeysPerNode;

    /// Emit a node at the given tree level, and add a reference to
    /// the emitted node to its parent.  The highest emitted level is
    /// the tree height, and the last emitted node is the root node.
    /// After the node is emitted, it is cleared.
    void emitLevel(int level)
    {
        // Get position of this node.  The last emitted node is always
        // the root
        trailer.rootPos = out->tell();

        // Update tree height
        if((int)trailer.treeHeight <= level)
            trailer.treeHeight = level + 1;
        
        // Emit this node
        levels[level]->emit(out);

        // Add a link to this node to the next level above (which may
        // or may not be emitted later)
        addKey(levels[level]->getFirstKey(), trailer.rootPos, level+1);

        // Clear out this node
        levels[level]->reset();
    }

    /// Add the given (key,pos) pair to the tree at the given level.
    /// If the level is full, emit the node first, and add the key to
    /// a fresh node at the same level.
    void addKey(key_t const & key, pos_t pos, size_t level)
    {
        // Build out necessary levels
        while(level >= levels.size())
            levels.push_back(new builder_t(maxKeysPerNode));

        // If the current node at this level is full, emit it, add it
        // to the next level up, and reset it
        if(levels[level]->full())
            emitLevel(level);

        // Add the key/pos pair to the current node at this level
        levels[level]->addKey(key, pos);
    }

public:
    BTreeBuilder(FilePtr const & out,
                 uint32_t maxNodeSize,
                 pos_t minSeparation,
                 uint32_t keyType = BTreeValueTraits<key_t>::type,
                 uint16_t keyVersion = BTreeValueTraits<key_t>::version,
                 uint16_t posType = BTreeValueTraits<pos_t>::type) :
        out(out),
        lastPos(0),
        minSeparation(minSeparation)
    {
        using namespace ex;

        if(!out)
            raise<ValueError>("null output file");

        // Init trailer.  With tree height of 0, the trailer is
        // effectively a leaf.  The rootPos should point to the data
        // start offset (i.e. the beginning of the file)
        trailer.rootPos = pos_t(0);
        trailer.maxNodeSize = maxNodeSize;
        trailer.treeHeight = 0;

        // Compute keys per node
        maxKeysPerNode = 0;
        size_t nodeSz = alignUp(sizeof(node_t), 8);
        if(maxNodeSize >= nodeSz)
            maxKeysPerNode = (maxNodeSize - nodeSz) / sizeof(item_t);

        if(maxKeysPerNode <= 0)
            raise<RuntimeError>("minimum node size is %1% bytes",
                                nodeSz + sizeof(item_t));

        // Emit header
        BTreeHeader hdr = {
            BTreeHeader::MAGIC,
            BTreeHeader::VERSION,
            sizeof(trailer_t),
            keyType,
            keyVersion,
            posType,
            0
        };
        size_t hdrSz = out->write(&hdr, sizeof(hdr));
        if(hdrSz < sizeof(hdr))
        {
            raise<IOError>("BTreeBuilder emitted %1% of %2% header bytes",
                           hdrSz, sizeof(hdr));
        }
    }

    ~BTreeBuilder()
    {
        // Finish, if we haven't already
        if(out)
            finish();

        // Clean up NodeBuilders
        std::for_each(levels.begin(), levels.end(), delete_ptr());
    }


    /// Add a (key, pos) pair to the tree.  Keys must be added in
    /// non-decreasing order.  Only the first key of each run of
    /// identical key values is added.  Also, the key will not be
    /// added if its position is less than minSeparation from the last
    /// added key's position.  Returns true if the key is added to the
    /// tree.
    bool addKey(key_t const & key, pos_t pos)
    {
        // Verify key ordering
        if(!levels.empty())
        {
            if(key < lastKey)
                ex::raise<ex::RuntimeError>
                    ("BTree keys must be added in non-decreasing order");
            if(!(lastKey < key))
                return false;
        }
        lastKey = key;

        // Filter for key separation
        if(levels.empty())
        {
            // First key.  Always add it.  It's also our trailer
            // rootPos until we emit a node.
            lastPos = pos;
            addKey(key, pos, 0);
            trailer.rootPos = pos;
            return true;
        }
        else if(pos >= lastPos + minSeparation)
        {
            // Meets separation requirements
            lastPos = pos;
            addKey(key, pos, 0);
            return true;
        }
        else
            return false;
    }
    
    /// Finish the tree and emit the trailer.  Returns the final tree
    /// height.
    uint32_t finish()
    {
        for(int li = 0, nLevels = levels.size(); li < nLevels; ++li)
        {
            // Emit all partially built levels
            if(!levels[li]->empty())
                emitLevel(li);
        }

        // Emit BTreeTrailer
        out->write(&trailer, sizeof(trailer_t));

        // We're done -- don't allow reuse of this object.
        out.reset();

        // Return tree height
        return trailer.treeHeight;
    }
};


#endif // WARP_BTREEBUILDER_H
