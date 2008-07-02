//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/oort/serializer.h#1 $
//
// Created 2006/05/24
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef OORT_SERIALIZER_H
#define OORT_SERIALIZER_H

#include "record.h"
#include "recordstream.h"
#include <boost/scoped_array.hpp>
#include <boost/utility.hpp>

namespace oort
{
    //------------------------------------------------------------------------
    // InputStreamSerializer
    //------------------------------------------------------------------------
    /// Convert a Record input stream to a byte stream.
    class InputStreamSerializer : public boost::noncopyable
    {
        typedef boost::scoped_array<char> buf_t;
    
        RecordStreamHandle input;
        HeaderSpec const * spec;
        buf_t headerBuf;

        Record rec;
        size_t headerLeft;
        size_t dataLeft;
    
    public:
        InputStreamSerializer(RecordStreamHandle const & input,
                              HeaderSpec const * spec);
    
        size_t read(void * dst, size_t sz);
    };
}

#endif // OORT_SERIALIZER_H
