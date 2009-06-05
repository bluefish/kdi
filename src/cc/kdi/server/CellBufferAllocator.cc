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

#include <kdi/server/CellBufferAllocator.h>
#include <kdi/server/CellBuffer.h>
#include <boost/bind.hpp>

using namespace kdi::server;

//----------------------------------------------------------------------------
// CellBufferAllocator
//----------------------------------------------------------------------------
CellBufferAllocator::CellBufferAllocator(size_t maxSize) :
    maxSize(maxSize),
    allocSz(0),
    nObjects(0)
{
}

CellBufferCPtr CellBufferAllocator::allocate(strref_t data)
{
    boost::mutex::scoped_lock lock(mutex);
    while(nObjects && allocSz + data.size() > maxSize)
        objReleased.wait(lock);
        
    ++nObjects;
    allocSz += data.size();

    CellBufferCPtr p(
        new CellBuffer(data),
        boost::bind(&CellBufferAllocator::release, this, _1));
    return p;
}

void CellBufferAllocator::release(CellBuffer const * p)
{
    boost::mutex::scoped_lock lock(mutex);
    
    assert(nObjects);
    assert(allocSz >= p->getPacked().size());

    --nObjects;
    allocSz -= p->getPacked().size();
    delete p;
    objReleased.notify_all();
}
