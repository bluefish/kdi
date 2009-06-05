//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-06-04
//
// This file is part of KDI.
//
// KDI is free software; you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the Free Software
// Foundation; either version 2 of the License, or any later version.
//
// KDI is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
// details.
//
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef KDI_SERVER_CELLBUFFERALLOCATOR_H
#define KDI_SERVER_CELLBUFFERALLOCATOR_H

#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <kdi/strref.h>

namespace kdi {
namespace server {

    class CellBufferAllocator;

    // Forward declarations
    class CellBuffer;
    typedef boost::shared_ptr<CellBuffer const> CellBufferCPtr;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// CellBufferAllocator
//----------------------------------------------------------------------------
class kdi::server::CellBufferAllocator
    : private boost::noncopyable
{
public:
    explicit CellBufferAllocator(size_t maxSize);

    /// Allocate a new CellBuffer with the given data.  If there isn't
    /// enough memory available to service the request, this call will
    /// block until there is.
    CellBufferCPtr allocate(strref_t data);

private:
    void release(CellBuffer const * p);
    
private:
    size_t const maxSize;
    size_t allocSz;
    size_t nObjects;
    boost::mutex mutex;
    boost::condition objReleased;
};

#endif // KDI_SERVER_CELLBUFFERALLOCATOR_H
