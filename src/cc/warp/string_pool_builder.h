//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/string_pool_builder.h#1 $
//
// Created 2007/11/09
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_STRING_POOL_BUILDER_H
#define WARP_STRING_POOL_BUILDER_H

#include "ex/exception.h"
#include "util.h"
#include "strref.h"
#include "string_data.h"
#include "hashtable.h"
#include "strhash.h"
#include "builder.h"
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
        static str_data_t const & getStr(strref_t s) {
            return s;
        }
        static str_data_t getStr(StringData const * s) {
            return str_data_t(s->begin(), s->end());
        }
        static str_data_t getStr(BuilderBlock const * b) {
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
