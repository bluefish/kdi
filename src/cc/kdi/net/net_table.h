//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/net/net_table.h#2 $
//
// Created 2007/12/12
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_NET_NET_TABLE_H
#define KDI_NET_NET_TABLE_H

#include <kdi/table.h>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <string>

namespace kdi {
namespace net {

    class NetTable;

} // namespace net
} // namespace kdi

//----------------------------------------------------------------------------
// NetTable
//----------------------------------------------------------------------------
class kdi::net::NetTable
    : public kdi::Table,
      private boost::noncopyable
{
    class Impl;
    class Scanner;

    boost::shared_ptr<Impl> impl;

public:
    NetTable(std::string const & uri);
    virtual ~NetTable();

    // Table interface
    virtual void set(strref_t row, strref_t column, int64_t timestamp,
                     strref_t value);
    virtual void erase(strref_t row, strref_t column, int64_t timestamp);
    virtual CellStreamPtr scan() const;
    virtual CellStreamPtr scan(ScanPredicate const & pred) const;
    virtual void sync();
};


#endif // KDI_NET_NET_TABLE_H
