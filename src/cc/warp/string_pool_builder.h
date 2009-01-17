//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-11-09
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

#ifndef WARP_STRING_POOL_BUILDER_H
#define WARP_STRING_POOL_BUILDER_H

#include <ex/exception.h>
#include <warp/util.h>
#include <warp/string_range.h>
#include <warp/string_data.h>
#include <warp/hashtable.h>
#include <warp/strhash.h>
#include <warp/builder.h>
#include <boost/noncopyable.hpp>

namespace warp {

    class StringPoolBuilder;

} // namespace warp

//----------------------------------------------------------------------------
// StringPoolBuilder
//----------------------------------------------------------------------------
class warp::StringPoolBuilder
    : private boost::noncopyable
{
    // StringBlockHash
    struct StringBlockHash : public warp::HsiehHash
    {
        BuilderBlock ** data;
        explicit StringBlockHash(BuilderBlock ** data) : data(data) {}

        using HsiehHash::operator();

        result_t operator()(StringData const * s) const {
            return operator()(s->begin(), s->end());
        }

        result_t operator()(size_t pos) const {
            return operator()(
                reinterpret_cast<StringData const *>(
                    (*data)->begin() + pos
                    )
                );
        }
    };

    // StringBlockEq
    struct StringBlockEq
    {
        BuilderBlock ** data;
        explicit StringBlockEq(BuilderBlock ** data) : data(data) {}

        strref_t getStr(strref_t s) const {
            return s;
        }

        StringRange getStr(StringData const * s) const {
            return StringRange(s->begin(), s->end());
        }

        StringRange getStr(size_t pos) const {
            return getStr(
                reinterpret_cast<StringData const *>(
                    (*data)->begin() + pos
                    )
                );
        }

        template <class A, class B>
        bool operator()(A const & a, B const & b) const {
            return getStr(a) == getStr(b);
        }
    };

    typedef HashTable<size_t, StringBlockHash,
                      StringBlockHash::result_t,
                      StringBlockEq> table_t;

    table_t pool;
    BuilderBlock * block;

public:
    /// Construct a StringPoolBuilder over the given BuilderBlock.
    explicit StringPoolBuilder(BuilderBlock * builder) :
        pool(0.75, StringBlockHash(&block), StringBlockEq(&block))
    {
        reset(builder);
    }

    /// Get the block containing the pooled string data.
    BuilderBlock * getStringBlock()
    {
        return block;
    }

    /// Get the offset into the string block for the given string.  If
    /// the string already exists in the pool, the offset of the
    /// previous instance is returned.  Otherwise, a new StringData is
    /// added to the string block and its offset is returned.
    size_t getStringOffset(strref_t s)
    {
        // Look in the existing pool, and return the match if we find
        // one
        table_t::const_iterator i = pool.find(s);
        if(i != pool.end())
            return *i;
 
        // Make a new StringData block
        block->appendPadding(4);
        size_t pos = block->size();
        *block << StringData::wrap(s);
    
        // Add it to the pool, and return the new block
        pool.insert(pos);
        return pos;
    }

    /// Get the string from the builder for given offset
    StringRange getString(size_t pos) const {
        StringData const * s = reinterpret_cast<StringData const *>(block->begin() + pos);
        return StringRange(s->begin(), s->end());    
    }

    /// Clear out the pool and reset with a new builder.
    void reset(BuilderBlock * builder)
    {
        EX_CHECK_NULL(builder);

        block = builder->subblock(4);
        pool.clear();
    }
    
    /// Get approximate data size of pool.
    size_t getDataSize() const { return block->size(); }
};


#endif // WARP_STRING_POOL_BUILDER_H
