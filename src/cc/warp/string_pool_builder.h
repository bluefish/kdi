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
        using HsiehHash::operator();

        result_t operator()(StringData const * s) const {
            return operator()(s->begin(), s->end());
        }

        result_t operator()(BuilderBlock const * b) const {
            return operator()(reinterpret_cast<StringData const *>(b->begin()));
        }
    };

    // StringBlockEq
    struct StringBlockEq
    {
        static strref_t getStr(strref_t s) {
            return s;
        }
        static StringRange getStr(StringData const * s) {
            return StringRange(s->begin(), s->end());
        }
        static StringRange getStr(BuilderBlock const * b) {
            return getStr(reinterpret_cast<StringData const *>(b->begin()));
        }

        template <class A, class B>
        bool operator()(A const & a, B const & b) const {
            return getStr(a) == getStr(b);
        }
    };

    typedef HashTable<BuilderBlock *, StringBlockHash,
                      StringBlockHash::result_t,
                      StringBlockEq> table_t;

    table_t pool;
    BuilderBlock * builder;
    size_t dataSz;

public:
    /// Construct a StringPoolBuilder over the given BuilderBlock.
    explicit StringPoolBuilder(BuilderBlock * builder) { reset(builder); }

    /// Get a block containing StringData for the given string.  If
    /// the string already exists in the pool, the previous instance
    /// is returned.  Otherwise, a new string is added.
    BuilderBlock * get(strref_t s)
    {
        // Look in the existing pool, and return the match if we find one
        table_t::const_iterator i = pool.find(s);
        if(i != pool.end())
            return *i;
 
        // Make a new StringData block
        BuilderBlock * b = builder->subblock(4);
        *b << StringData::wrap(s);
    
        // Update data size -- align up to account for possible padding
        dataSz += 4 + alignUp(s.size(), 4);

        // Add it to the pool, and return the new block
        pool.insert(b);
        return b;
    }

    /// Clear out the pool and reset with a new builder.
    void reset(BuilderBlock * builder)
    {
        EX_CHECK_NULL(builder);

        this->builder = builder;
        pool.clear();
        dataSz = 0;
    }
    
    /// Clear out the pool and reuse the same builder.
    void reset() { reset(builder); }

    /// Get the BuilderBlock backing this pool.
    BuilderBlock * getBuilder() const { return builder; }

    /// Get approximate data size of pool.
    size_t getDataSize() const { return dataSz; }
};


#endif // WARP_STRING_POOL_BUILDER_H
