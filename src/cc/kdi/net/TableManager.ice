//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-10
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

#ifndef KDI_NET_TABLEMANAGER_ICE
#define KDI_NET_TABLEMANAGER_ICE

#include <Ice/BuiltinSequences.ice>

module kdi {
module net {
module details {

    interface Scanner {
        void getBulk(out Ice::ByteSeq cells, out bool lastBlock);
        idempotent void close();
    };

    interface Table {
        idempotent void applyMutations(Ice::ByteSeq cells);
        idempotent void sync();
        idempotent Scanner* scan(string predicate);
    };

    interface TableManager {
        Table* openTable(string path);
    };

}; // module details
}; // module net
}; // module kdi


#endif // KDI_NET_TABLEMANAGER_ICE
