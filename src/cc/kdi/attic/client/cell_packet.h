//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/attic/client/cell_packet.h#1 $
//
// Created 2007/11/07
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
