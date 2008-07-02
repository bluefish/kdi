//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/oort/headers.h#1 $
//
// Created 2006/02/17
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef OORT_HEADERS_H
#define OORT_HEADERS_H

#include "warp/util.h"
#include "record.h"
#include <boost/static_assert.hpp>
#include <assert.h>

namespace oort
{
    //------------------------------------------------------------------------
    // CastingHeaderSpec
    //------------------------------------------------------------------------
    template <class Header>
    class CastingHeaderSpec : public HeaderSpec
    {
    public:
        size_t headerSize() const { return sizeof(Header); }

        void serialize(Fields const & src, void * dst) const
        {
            Header * hdr = reinterpret_cast<Header *>(dst);
            hdr->setLength(src.length);
            hdr->setType(src.type);
            hdr->setVersion(src.version);
            hdr->setFlags(src.flags);
        }

        void deserialize(void const * src, Fields & dst) const
        {
            Header const * hdr = reinterpret_cast<Header const *>(src);
            dst.length = hdr->getLength();
            dst.type = hdr->getType();
            dst.version = hdr->getVersion();
            dst.flags = hdr->getFlags();
        }
    };

    namespace vega
    {
#pragma pack(push, 1)
        //--------------------------------------------------------------------
        // vega::RecordHeader
        //--------------------------------------------------------------------
        struct RecordHeader
        {
            typedef CastingHeaderSpec<RecordHeader> Spec;
            enum { ALIGNMENT = 4 };

            uint32_t length;
            uint32_t type;
            uint8_t  version;
            uint8_t  flags;

            uint32_t getLength() const  { return length; }
            uint32_t getType() const    { return type; }
            uint32_t getVersion() const { return version; }
            uint32_t getFlags() const   { return flags; }

            void setLength(uint32_t n)  { length = n; }
            void setType(uint32_t n)    { type = n; }
            void setVersion(uint32_t n) { version = n; }
            void setFlags(uint32_t n)   { flags = n; }
        };
#pragma pack(pop)

        BOOST_STATIC_ASSERT(sizeof(RecordHeader) == 10);
    }

    namespace sirius
    {
        //--------------------------------------------------------------------
        // sirius::RecordHeader
        //--------------------------------------------------------------------
        struct RecordHeader
        {
            typedef CastingHeaderSpec<RecordHeader> Spec;
            enum { ALIGNMENT = 1 };

            char data[10];

            uint32_t getLength() const  { return warp::byteswap(warp::deserialize<uint32_t>(&data[6])); }
            uint32_t getType() const    { return warp::deserialize<uint32_t>(&data[0]); }
            uint32_t getVersion() const { return warp::byteswap(warp::deserialize<uint16_t>(&data[4])); }
            uint32_t getFlags() const   { return 0; }

            void setLength(uint32_t n)  { warp::serialize(&data[6], warp::byteswap(n)); }
            void setType(uint32_t n)    { warp::serialize(&data[0], n); }
            void setVersion(uint32_t n) { warp::serialize(&data[4], warp::byteswap<uint16_t>(n)); }
            void setFlags(uint32_t n)   { assert(n == 0); }
        };
    }


    extern vega::RecordHeader::Spec   const * const VEGA_SPEC;
    extern sirius::RecordHeader::Spec const * const SIRIUS_SPEC;
}

#endif // OORT_HEADERS_H
