//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-03
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

#ifndef KDI_SERVER_FILELOG_H
#define KDI_SERVER_FILELOG_H

#include <warp/pack.h>

namespace kdi {
namespace server {

    struct FileLogHeader
    {
        enum { MAGIC = WARP_PACK4('f','l','o','g') };
        uint32_t magic;         // equals FileLogHeader::MAGIC
        uint32_t version;       // version of entry headers to expect
    };

    struct FileLogEntryHeaderV0
    {
        enum {
            MAGIC = WARP_PACK4('f','e','h','0'),
            MAX_NAME_LEN = 2048,
            MAX_DATA_LEN = 32 << 20,
        };

        uint32_t magic;         // equals FileLogEntryHeaderV0::MAGIC
        uint32_t checksum;      // adler32 of (nameLen + dataLen + name)
        uint32_t nameLen;       // length of table name (follows header)
        uint32_t dataLen;       // length of cell buffer (follows name)
        // char name[nameLen];
        // char cells[dataLen];
    };

} // namespace server
} // namespace kdi

#endif // KDI_SERVER_FILELOG_H
