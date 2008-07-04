//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-04-06
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef WARP_BTREEREADER_H
#define WARP_BTREEREADER_H

#include <warp/btree.h>
#include <warp/file.h>
#include <ex/exception.h>
#include <boost/shared_ptr.hpp>
#include <assert.h>

namespace warp
{
    template <class K, class P=uint64_t, size_t MAX_NODE_SZ=16384>
    class BTreeReader;
}

//----------------------------------------------------------------------------
// BTreeReader
//----------------------------------------------------------------------------
/// Wrapper to interpret a readable, random-access File handle as a
/// BTree.  Note this reader has a hard-coded maximum node size given
/// in the parameter \c MAX_NODE_SZ.  This is not strictly necessary,
/// but it allows for more efficient lookups.
template <class K, class P, size_t MAX_NODE_SZ>
class warp::BTreeReader
{
public:
    typedef K                  key_t;
    typedef P                  pos_t;
    typedef BTreeItem<K,P>     item_t;
    typedef BTreeNode<K,P>     node_t;
    typedef BTreeTrailer<P>    trailer_t;
    typedef BTreeHeader        header_t;

    typedef BTreeReader<K,P,MAX_NODE_SZ> my_t;
    typedef boost::shared_ptr<my_t> handle_t;

private:
    FilePtr fp;
    trailer_t trailer;

public:
    explicit BTreeReader(FilePtr const & fp, bool checkKey=true) :
        fp(fp)
    {
        using namespace ex;

        if(!fp)
            raise<RuntimeError>("null index file");
        assert(fp->tell() == 0);
        
        // Read header
        header_t hdr;
        if(!fp->read(&hdr, sizeof(hdr), 1))
            raise<IOError>("couldn't read header: %1%:%2%",
                           fp->getName(), fp->tell());

        // Check the fields we can.  Assume the user has done
        // appropriate checking for keyType and user-defined posType.
        if(hdr.magic != header_t::MAGIC)
            raise<RuntimeError>("wrong BTree magic number");

        if(hdr.version != header_t::VERSION)
            raise<RuntimeError>("wrong BTree version");

        if(checkKey && hdr.keyType != BTreeValueTraits<key_t>::type)
            raise<RuntimeError>("wrong key type");

        if(checkKey && hdr.keyVersion != BTreeValueTraits<key_t>::version)
            raise<RuntimeError>("wrong key version");

        if(!(hdr.posType & header_t::MASK_USER_DEFINED) &&
           hdr.posType != BTreeValueTraits<pos_t>::type)
            raise<RuntimeError>("wrong position type");

        if(hdr.trailerSize != sizeof(trailer))
            raise<RuntimeError>("wrong trailer size: %1%",
                                        hdr.trailerSize);

        // Read trailer
        fp->seek(-off_t(sizeof(trailer)), SEEK_END);
        if(!fp->read(&trailer, sizeof(trailer), 1))
            raise<IOError>("couldn't read trailer: %1%:%2%",
                           fp->getName(), fp->tell());
        if(trailer.maxNodeSize < sizeof(node_t))
            raise<RuntimeError>("max node size is less than minimal node "
                                "size: %1%", trailer.maxNodeSize);
        if(trailer.maxNodeSize > MAX_NODE_SZ)
            raise<RuntimeError>("max node size is too large: %1%",
                                trailer.maxNodeSize);
    }

    /// Look up the position associated with the given key.  Note that
    /// this function is not thread-safe: it does seeks and reads on
    /// the index file object.
    /// @param key Key to lookup
    /// @param lt Ordering on lookup key and index key types
    /// @return Nearest position in indexed file before \c key
    template <class KK, class Lt>
    pos_t lookup(KK const & key, Lt const & lt) const
    {
        using namespace ex;

        char nodeBuf[MAX_NODE_SZ];
        node_t const * node = reinterpret_cast<node_t const *>(nodeBuf);

        pos_t pos = trailer.rootPos;
        for(int i = trailer.treeHeight; i > 0; --i)
        {
            // Read next node
            fp->seek(pos);
            size_t nodeSz = fp->read(nodeBuf, trailer.maxNodeSize);

            // Validate node
            if(nodeSz < sizeof(node_t))
                raise<IOError>("failed to read node: %1%:%2%",
                               fp->getName(), pos);
            char const * nodeEnd = reinterpret_cast<char const *>
                (node->items.end());
            if(nodeEnd > nodeBuf + nodeSz)
                raise<IOError>("read short node (%1% of %2% bytes): %3%:%4%",
                               nodeSz, nodeEnd - nodeBuf, fp->getName(), pos);

            // Lookup position
            pos = node->lookup(key, lt);
        }

        return pos;
    }

    /// Look up the position associated with the given key.  Note that
    /// this function is not thread-safe: it does seeks and reads on
    /// the index file object.
    /// @param key Key to lookup
    /// @return Nearest position in indexed file before \c key
    pos_t lookup(key_t const & key) const
    {
        return lookup(key, std::less<key_t>());
    }
};


#endif // WARP_BTREEREADER_H
