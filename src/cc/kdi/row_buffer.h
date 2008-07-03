//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-01-20
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
