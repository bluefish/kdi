//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-31
//
// This file is part of the warp library.
//
// The warp library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
//
// The warp library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef WARP_STRINGTRIE_H
#define WARP_STRINGTRIE_H

#include <warp/StringAlloc.h>
#include <warp/PointerValueStore.h>
#include <warp/string_range.h>
#include <warp/objectpool.h>
#include <warp/tuple.h>
#include <boost/noncopyable.hpp>
#include <boost/scoped_array.hpp>
#include <algorithm>
#include <string.h>

namespace warp {

    template <class T>
    class StringTrie;

} // namespace warp

//----------------------------------------------------------------------------
// StringTrie
//----------------------------------------------------------------------------
template <class T>
class warp::StringTrie
    : private boost::noncopyable
{
public:
    size_t nNodeCmp;
    size_t nCharCmp;
    size_t nNodes;

public:
    StringTrie() {}

    void insert(strref_t key, T const & val)
    {
        insert(key.begin(), key.end());
        //Node * n = insert(key.begin(), key.end());
        //valueStore.write(val, n->child);
    }

private:

    struct Node;


    struct NodeLt
    {
        size_t * nNodeCmp;

        NodeLt(size_t * nNodeCmp) : nNodeCmp(nNodeCmp) {}

        bool operator()(Node const * a, Node const * b) const
        {
            ++(*nNodeCmp);

            if(a->length)
            {
                if(b->length)
                    return a->data[0] < b->data[0];
                else
                    return false;
            }
            else if(b->length)
                return true;
            else
                return false;
        }

        bool operator()(char a, Node const * b) const
        {
            ++(*nNodeCmp);

            if(b->length)
                return a < b->data[0];
            else
                return false;
        }

        bool operator()(Node const * a, char b) const
        {
            ++(*nNodeCmp);

            if(a->length)
                return a->data[0] < b;
            else
                return true;
        }
    };

    struct Node
    {
        char const * data;
        boost::scoped_array<Node *> children;
        uint32_t length;
        uint16_t nChildren;
        uint8_t hasNullChild;

        Node() : data(0), length(0), nChildren(0), hasNullChild(0) {}

        void swapChildren(Node & o)
        {
            children.swap(o.children);
            std::swap(nChildren, o.nChildren);
            std::swap(hasNullChild, o.hasNullChild);
        }

        Node * findChild(char const * a, char const * b, size_t * nNodeCmp)
        {
            if(a == b)
            {
                if(hasNullChild)
                    return children[0];
                else
                    return 0;
            }
            
            Node ** begin = children.get();
            Node ** end = begin + nChildren;
            Node ** i = std::lower_bound(begin, end, *a, NodeLt(nNodeCmp));
            if(i != end && (*i)->data[0] == *a)
                return *i;
            else
                return 0;
        }

        void addChild(Node * c, size_t * nNodeCmp)
        {
            Node ** begin = children.get();
            Node ** end = begin + nChildren;
            Node ** i = std::lower_bound(begin, end, c, NodeLt(nNodeCmp));

            boost::scoped_array<Node *> newChildren(new Node *[nChildren+1]);
            Node ** out = newChildren.get();
            out = std::copy(begin, i, out);
            *out = c;
            std::copy(i, end, ++out);

            children.swap(newChildren);

            if(!c->length)
                hasNullChild = 1;
            ++nChildren;
        }
    };


private:
    // Some notes:
    //
    // When building the tree, we need to store child and sibling
    // pointers, since we may have to splice a new node into either
    // list.  Once the trie is finished, we can convert it to a
    // compacted form with better search properties, though.  Arrange
    // the trie node list so that the sibling node is always the next
    // node in the node block (e.g. this + 1).  Each node can store a
    // byte containing the number of children the node has.  Actually
    // maybe it should be 9 bits, since it's possible to have 257
    // children (one for each subsequent byte plus an empty string).
    // Anyway, the count determines how far to take the sibling list,
    // which starts at child and ends at child + nChildren.  The
    // siblings can also be sorted to allow a binary search for
    // branching.  If we have an extra byte in the node, it might make
    // sense to copy the first byte of the segment string into the
    // node to gain memory locality for the search.  If nChildren is
    // 0, then the node is a leaf and the child pointer can be used to
    // store some associated leaf data instead of a pointer to another
    // node.

private:
    Node * branch(Node * node, char const * branchPt,
                  char const * tailBegin, char const * tailEnd)
    {
        Node * mid = nodeAlloc.create();
        Node * sib = nodeAlloc.create();

        nNodes += 2;

        mid->data = branchPt;
        mid->length = node->length - (branchPt - node->data);
        mid->swapChildren(*node);

        sib->length = tailEnd - tailBegin;
        char * d = stringAlloc.alloc(sib->length);
        memcpy(d, tailBegin, sib->length);
        sib->data = d;

        node->length -= mid->length;
        node->addChild(mid, &nNodeCmp);
        node->addChild(sib, &nNodeCmp);

        return sib;
    }

    Node * insert(char const * begin, char const * end)
    {
        // Find a node where the input string diverges from the
        // contained string
        Node * p = &root;

        // Find the amount of overlap for the insertion string and the
        // containing node.  If it's zero, add a sibling node.  If
        // it's more, create two new child nodes.  The first will
        // point to the mismatched suffix of this node and become the
        // parent of this node's child, if any.  The second will
        // contain the newly copied mismatched suffix of the query
        // string and become a sibling of the first child node.  This
        // node will be shortened to the matching prefix length and
        // update it's child pointer to the first new child.

        Node * n = p->findChild(begin, end, &nNodeCmp);
        while(n)
        {
            char const * ni;
            char const * si;
            if(n->length >= size_t(end - begin))
            {
                tie(si,ni) = std::mismatch(begin, end, n->data);

                nCharCmp += ni - n->data;

                if(ni == n->data + n->length)
                    return n;
                else
                    return branch(n, ni, si, end);
            }

            tie(ni,si) = std::mismatch(
                n->data, n->data + n->length, begin);

            nCharCmp += ni - n->data;

            if(ni != n->data + n->length)
                return branch(n, ni, si, end);

            begin = si;
            p = n;
            n = p->findChild(begin, end, &nNodeCmp);
        }

        n = nodeAlloc.create();
        n->length = end - begin;
        char * d = stringAlloc.alloc(n->length);
        memcpy(d, begin, n->length);
        n->data = d;
        
        p->addChild(n, &nNodeCmp);

        ++nNodes;
        
        return n;
    }

    //bool contains(char const * begin, char const * end) const
    //{
    //    // Null root means the trie contains nothing, not even the
    //    // empty string.  To contain the empty string, it must be
    //    // inserted explicitly.  It will be represented as a
    //    // zero-length sibling of the root.
    //
    //    // First find the child of the node that exactly contains the
    //    // tail of the query string.
    //
    //    Node * n = root;
    //    while(begin != end)
    //    {
    //        // Out of nodes?  No match.
    //        if(!n)
    //            return false;
    //
    //        // If this node has the wrong prefix, move to the next
    //        // sibling.
    //        if(!n->length || n->data[0] != begin[0])
    //        {
    //            n = n->sibling;
    //            continue;
    //        }
    //
    //        // If this node is longer than the remaining query string,
    //        // then we may have a prefix, but we cannot have an exact
    //        // match.
    //        if(n->length > size_t(end - begin))
    //            return false;
    //
    //        // Match the string segment in this node.  Mismatch means
    //        // we don't have the query string.
    //        if(memcmp(begin, n->data, n->length))
    //            return false;
    //
    //        // This node matched the (remaining) prefix of the query
    //        // string.  Move the query start pointer up to the end of
    //        // the matched prefix and descend to the child of this
    //        // node.
    //        begin += n->length;
    //        n = n->child;
    //    }
    //
    //    // The remaining query string is empty.  If the current node
    //    // is null, we have an exact match (except in the special case
    //    // of an empty query string and a null root).
    //    if(root && !n)
    //        return true;
    //
    //    // There are nodes at the level.  We have an exact match iff
    //    // one of the siblings is zero-length.
    //    while(n)
    //    {
    //        if(!n->length)
    //            return true;
    //        n = n->sibling;
    //    }
    //
    //    // No exact match.
    //    return false;
    //}

private:
    ObjectPool<Node> nodeAlloc;
    StringAlloc stringAlloc;
    PointerValueStore<T> valueStore;
    Node root;
};


#endif // WARP_STRINGTRIE_H
