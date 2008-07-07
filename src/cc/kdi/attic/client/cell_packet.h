//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-11-07
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

#ifndef KDI_CLIENT_CELL_PACKET_H
#define KDI_CLIENT_CELL_PACKET_H

namespace kdi {
namespace client {

    struct CellPacket;
    class CellPacketBuffer;
    typedef boost::shared_ptr<CellPacketBuffer> CellPacketBufferPtr;

} // namespace client
} // namespace kdi

struct CellPacket
{
    ArrayOffset
};


#endif // KDI_CLIENT_CELL_PACKET_H
