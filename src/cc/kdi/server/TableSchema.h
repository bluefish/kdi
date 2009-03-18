//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-13
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

#ifndef KDI_SERVER_TABLESCHEMA_H
#define KDI_SERVER_TABLESCHEMA_H

#include <string>
#include <vector>

namespace kdi {
namespace server {

    struct TableSchema
    {
        struct Group
        {
            std::string name;
            std::string compressor;
            std::vector<std::string> columns; // empty() means "include all"
            int64_t maxAge;                   // <= 0 means "keep all"
            int64_t maxHistory;               // <= 0 means "keep all"
            size_t diskBlockSize;
            bool inMemory;

            Group() :
                maxAge(0),
                maxHistory(0),
                diskBlockSize(64<<10),
                inMemory(false)
            {
            }
        };

        std::string tableName;
        std::vector<Group> groups;
    };

} // namespace server
} // namespace kdi

#endif // KDI_SERVER_TABLESCHEMA_H
