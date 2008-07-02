//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: row_buffer.h $
//
// Created 2008/01/20
//
// Copyright 2008 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_ROW_BUFFER_H
#define KDI_ROW_BUFFER_H

#include <sdstore/rowbuffer.h>
#include <flux/stream.h>
#include <boost/shared_ptr.hpp>

namespace kdi
{
    using sdstore::RowBuffer;

    /// Shared pointer to a RowBuffer.
    typedef boost::shared_ptr<RowBuffer> RowBufferPtr;

    /// Stream over RowBuffers.
    typedef flux::Stream<RowBuffer> RowBufferStream;

    /// Shared pointer to a RowBufferStream.
    typedef boost::shared_ptr<RowBufferStream> RowBufferStreamPtr;
}

#endif // KDI_ROW_BUFFER_H
