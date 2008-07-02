//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/btree.h#1 $
//
// Created 2006/04/05
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_BTREE_H
#define WARP_BTREE_H

#include "pack.h"
#include "offset.h"
#include <stdint.h>
#include <algorithm>
#include <boost/type_traits/is_same.hpp>
#include <boost/utility/enable_if.hpp>

namespace warp
{
    struct BTreeHeader;

    template <class P=uint64_t>
    struct BTreeTrailer;

    template <class K, class P=uint64_t>
    struct BTreeItem;

    template <class K, class P=uint64_t>
    struct BTreeNode;

    template <class P> struct BTreeValueTraits;
    template <> struct BTreeValueTraits<uint8_t>;
    template <> struct BTreeValueTraits<uint16_t>;
    template <> struct BTreeValueTraits<uint32_t>;
    template <> struct BTreeValueTraits<uint64_t>;
    template <> struct BTreeValueTraits<int8_t>;
    template <> struct BTreeValueTraits<int16_t>;
    template <> struct BTreeValueTraits<int32_t>;
    template <> struct BTreeValueTraits<int64_t>;
}


//----------------------------------------------------------------------------
// BTreeHeader
//----------------------------------------------------------------------------
/// Disk header for BTree.  Contains information about how to
/// interpret the BTree.
struct warp::BTreeHeader
{
    enum {
        MAGIC = WARP_PACK4('I','D','X','!'),
        VERSION = 0,
    };

    uint32_t magic;             ///< Should equal BTreeHeader::MAGIC
    uint16_t version;           ///< Should equal BTreeHeader::VERSION
    uint16_t trailerSize;

    uint32_t keyType;           ///< User-defined key type
    uint16_t keyVersion;        ///< User-defined key type version
    uint8_t  posType;           ///< Position type (see below)
    uint8_t  _pad;

    // Position type can be one of the 8 basic integer types defined
    // below, or some user-defined type indicated by a set bit in any
    // of the MASK_USER_DEFINED bits.
    enum {
        MASK_SIZE_EXP     = 0x0003,  // bits: 0000 0011
        MASK_SIGNED       = 0x0004,  // bits: 0000 0100
        MASK_USER_DEFINED = 0xfff8,  // bits: 1111 1000

        POS_UINT8         = 0,
        POS_UINT16        = 1,
        POS_UINT32        = 2,
        POS_UINT64        = 3,
        POS_INT8          = 0 | MASK_SIGNED,
        POS_INT16         = 1 | MASK_SIGNED,
        POS_INT32         = 2 | MASK_SIGNED,
        POS_INT64         = 3 | MASK_SIGNED,
    };
};


//----------------------------------------------------------------------------
// BTreeValueTraits
//----------------------------------------------------------------------------
/// Helper class to convert common value types to BTreeHeader types
/// and versions.
template <class T>
struct warp::BTreeValueTraits {
    enum {
        type = T::TYPECODE,
        version = T::VERSION,
    };
};
    
template <> struct warp::BTreeValueTraits<uint8_t> {
    enum {
        type = BTreeHeader::POS_UINT8,
        version = 0,
    };
};

template <> struct warp::BTreeValueTraits<uint16_t> {
    enum {
        type = BTreeHeader::POS_UINT16,
        version = 0,
    };
};

template <> struct warp::BTreeValueTraits<uint32_t> {
    enum {
        type = BTreeHeader::POS_UINT32,
        version = 0,
    };
};

template <> struct warp::BTreeValueTraits<uint64_t> {
    enum {
        type = BTreeHeader::POS_UINT64,
        version = 0,
    };
};

template <> struct warp::BTreeValueTraits<int8_t> {
    enum {
        type = BTreeHeader::POS_INT8,
        version = 0,
    };
};

template <> struct warp::BTreeValueTraits<int16_t> {
    enum {
        type = BTreeHeader::POS_INT16,
        version = 0,
    };
};

template <> struct warp::BTreeValueTraits<int32_t> {
    enum {
        type = BTreeHeader::POS_INT32,
        version = 0,
    };
};

template <> struct warp::BTreeValueTraits<int64_t> {
    enum {
        type = BTreeHeader::POS_INT64,
        version = 0,
    };
};


//----------------------------------------------------------------------------
// BTreeTrailer
//----------------------------------------------------------------------------
/// Fixed-size end of the BTree serialized form.  Contains the final
/// tree height and the location of the tree root node, as well as the
/// maximum size of any node in the tree.  If the tree height is zero,
/// then the trailer has a leaf interpretation: the root position is
/// an address in the indexed file rather than an offset within the
/// BTree.
template <class P>
struct warp::BTreeTrailer
{
    typedef P pos_t;

    pos_t    rootPos;           ///< Position of root node
    uint32_t maxNodeSize;       ///< Max size of any node in BTree
    uint32_t treeHeight;        ///< Height of tree
};


//----------------------------------------------------------------------------
// BTreeItem
//----------------------------------------------------------------------------
/// A splitting key within a BTreeNode.  Associates a position with a key.
template <class K, class P>
struct warp::BTreeItem
{
    typedef K key_t;
    typedef P pos_t;
    typedef BTreeItem<K,P> my_t;

    /// Functor for key/item and item/item comparisons.
    template <class Lt=warp::less>
    struct Less
    {
        Lt lt;
        Less(Lt const & lt = Lt()) : lt(lt) {}

        static key_t const & key_of(my_t const & x) { return x.key; }

        template <class T> static
        typename boost::disable_if<boost::is_same<T, my_t>, T>::type const &
        key_of(T const & x) { return x; }
        
        template <class A, class B>
        bool operator()(A a, B b) const {
            return lt(key_of(a), key_of(b));
        }
    };

    key_t key;
    pos_t pos;

    BTreeItem() {}
    BTreeItem(key_t const & key, pos_t pos) :
        key(key), pos(pos) {}

    bool operator<(my_t const & o) const { return key < o.key; }
    bool operator<(key_t const & k) const { return key < k; }
};


//----------------------------------------------------------------------------
// BTreeNode
//----------------------------------------------------------------------------
/// Node of a BTree.  Each node contains some number of keys, N, and
/// N+1 child positions.  If the node is a leaf, the position should
/// be interpreted as an offset in the original file indexed.
/// Otherwise, the position is the location of the a child BTreeNode
/// in the BTree.
template <class K, class P>
struct warp::BTreeNode
{
    typedef BTreeItem<K,P>           item_t;
    typedef typename item_t::key_t   key_t;
    typedef typename item_t::pos_t   pos_t;

    pos_t firstPos;
    ArrayOffset<item_t> items;

    /// Get the position in this node associated with the key range
    /// containing the given key.
    template <class KK, class Lt>
    pos_t lookup(KK const & key, Lt const & lt) const
    {
        item_t const * i = std::upper_bound(
            items.begin(), items.end(), key,
            typename item_t::template Less<Lt>(lt));
        if(--i < items.begin())
            return firstPos;
        else
            return i->pos;
    }

    /// Get the position in this node associated with the key range
    /// containing the given key.
    pos_t lookup(key_t const & key) const
    {
        return lookup(key, std::less<key_t>());
    }
};


#endif // WARP_BTREE_H
