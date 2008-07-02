//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/net/TableManager.ice#2 $
//
// Created 2007/12/10
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
