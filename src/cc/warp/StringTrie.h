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

#include <vector>
#include <iostream>

namespace warp {

    template <class T>
    class StringTrie;

    namespace StringTrie_details {

        extern uint32_t const BUCKET_SIZES[];
        extern uint32_t const GROW_SIZES[];

    }

} // namespace warp

//----------------------------------------------------------------------------
// StringTrie
//----------------------------------------------------------------------------
template <class T>
class warp::StringTrie
    : private boost::noncopyable
{
public:
    StringTrie() : root(0,0) {}

    void insert(strref_t key, T const & val)
    {
        insert(key.begin(), key.end());
        //Node * n = insert(key.begin(), key.end());
        //valueStore.write(val, n->child);
    }

private:
    struct Node
    {
        enum { MAX_LENGTH = 65535 };

        char const * data;
        boost::scoped_array<Node *> children;
        uint16_t length;
        uint16_t hash;
        uint16_t nChildren;
        uint16_t sizeIdx;

        Node(char const * data, uint16_t length) :
            data(data),
            length(length),
            hash(length ? 1 + uint16_t(data[0]) : 0),
            nChildren(0),
            sizeIdx(0)
        {
        }        

        char const * begin() const { return data; }
        char const * end() const { return data + length; }

        void swapChildren(Node & o)
        {
            children.swap(o.children);
            std::swap(nChildren, o.nChildren);
            std::swap(sizeIdx, o.sizeIdx);
        }

        Node * findChild(char const * a, char const * b)
        {
            using namespace StringTrie_details;

            if(!nChildren)
                return 0;

            uint16_t h = (a != b ? 1 + uint16_t(*a) : 0);
            
            Node ** begin = &children[0];
            Node ** end = begin + BUCKET_SIZES[sizeIdx];
            Node ** i = begin + (h % BUCKET_SIZES[sizeIdx]);
            Node ** start = i;
            for(;;)
            {
                if(!*i)
                    return 0;
                
                if((*i)->hash == h)
                    return *i;

                if(++i == end)
                    i = begin;

                if(i == start)
                    return 0;
            }
        }

        void insert(Node * c)
        {
            using namespace StringTrie_details;

            uint16_t h = c->hash;
            Node ** i = &children[h % BUCKET_SIZES[sizeIdx]];
            Node ** end = children.get() + BUCKET_SIZES[sizeIdx];
            for(;;)
            {
                if(!*i || (*i)->hash == h)
                {
                    *i = c;
                    return;
                }

                if(++i == end)
                    i = children.get();
            }
        }

        void addChild(Node * c)
        {
            using namespace StringTrie_details;

            if(nChildren == GROW_SIZES[sizeIdx])
            {
                boost::scoped_array<Node *> old;
                old.swap(children);
                
                Node ** end = old.get() + BUCKET_SIZES[sizeIdx];

                ++sizeIdx;
                children.reset(new Node *[BUCKET_SIZES[sizeIdx]]);
                std::fill(children.get(), children.get() + BUCKET_SIZES[sizeIdx], (Node *)0);
                
                for(Node ** i = old.get(); i != end; ++i)
                {
                    if(*i)
                        insert(*i);
                }
            }

            ++nChildren;
            insert(c);
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
    Node * graft(Node * p, char const * begin, char const * end)
    {
        for(;;)
        {
            size_t sz = std::min<size_t>(end - begin, Node::MAX_LENGTH);

            char * d = stringAlloc.alloc(sz);
            memcpy(d, begin, sz);

            Node * n = nodeAlloc.create(d, sz);
            p->addChild(n);

            begin += sz;
            if(begin == end)
                return n;

            p = n;
        }
    }

    Node * branch(Node * node, char const * branchPt,
                  char const * tailBegin, char const * tailEnd)
    {
        Node * mid = nodeAlloc.create(
            branchPt,
            node->length - (branchPt - node->data));
        mid->swapChildren(*node);

        if(!(node->length -= mid->length))
            node->hash = 0;
        node->addChild(mid);

        return graft(node, tailBegin, tailEnd);
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

        Node * n = p->findChild(begin, end);
        while(n)
        {
            if(uint32_t(end - begin) <= n->length)
            {
                char const * ni = n->begin();
                char const * si = begin;
                for(; si != end; ++ni, ++si)
                {
                    if(*ni != *si)
                        break;
                }

                if(ni == n->end())
                    return n;
                else
                    return branch(n, ni, si, end);
            }

            char const * ni = n->begin() + 1;
            char const * si = begin + 1;
            char const * nEnd = n->end();
            for(; ni != nEnd; ++ni, ++si)
            {
                if(*ni != *si)
                    break;
            }

            if(ni != nEnd)
                return branch(n, ni, si, end);

            begin = si;
            p = n;
            n = p->findChild(begin, end);
        }

        return graft(p, begin, end);
    }


    struct DumpState
    {
        Node const * n;
        Node ** c;

        DumpState() : n(0), c(0) {}
        DumpState(Node const * n) : n(n), c(n->children.get()) {}
        
        Node const * next()
        {
            using namespace StringTrie_details;

            Node ** end = n->children.get() + BUCKET_SIZES[n->sizeIdx];

            while(c != end)
            {
                Node const * r = *c;
                ++c;
                if(r)
                    return r;
            }
            return 0;
        }
    };

public:
    void dump(std::ostream & o) const
    {
        std::vector<DumpState> stack;
        stack.push_back(&root);
        while(!stack.empty())
        {
            Node const * c = stack.back().next();
            if(c)
            {
                stack.push_back(c);
                continue;
            }

            for(size_t i = 0; i < stack.size()-1; ++i)
            {
                for(size_t j = 0; j < stack[i].n->length; ++j)
                    o << ' ';
            }
            o.write(stack.back().n->data, stack.back().n->length);
            o << ' ' << stack.size()
              << ' ' << stack.back().n->nChildren
              << std::endl;

            stack.pop_back();
        }
    }

private:

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
