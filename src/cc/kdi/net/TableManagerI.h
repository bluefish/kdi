//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/net/TableManagerI.h#2 $
//
// Created 2007/12/10
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_NET_TABLEMANAGERI_H
#define KDI_NET_TABLEMANAGERI_H

#include <warp/builder.h>
#include <warp/call_queue.h>
#include <kdi/net/TimeoutLocator.h>
#include <kdi/marshal/cell_block_builder.h>
#include <kdi/table.h>
#include <kdi/timestamp.h>
#include <boost/noncopyable.hpp>
#include <Ice/Identity.h>

// Generated header
#include <kdi/net/TableManager.h>

namespace kdi {
namespace net {
namespace details {

    class ScannerI;
    class TableI;
    class TableManagerI;

    typedef IceUtil::Handle<ScannerI> ScannerIPtr;
    typedef IceUtil::Handle<TableI> TableIPtr;
    typedef IceUtil::Handle<TableManagerI> TableManagerIPtr;

} // namespace details
} // namespace net
} // namespace kdi


//----------------------------------------------------------------------------
// ScannerI
//----------------------------------------------------------------------------
class kdi::net::details::ScannerI
    : virtual public kdi::net::details::Scanner
{
    kdi::CellStreamPtr scan;
    warp::Builder builder;
    kdi::marshal::CellBlockBuilder cellBuilder;

    kdi::Timestamp lastAccess;
    TimeoutLocatorPtr locator;

public:
    ScannerI(kdi::CellStreamPtr const & scan,
             TimeoutLocatorPtr const & locator);
    ~ScannerI();

    virtual void getBulk(Ice::ByteSeq & cells, bool & lastBlock,
                         Ice::Current const & cur);

    virtual void close(Ice::Current const & cur);

    kdi::Timestamp const & getLastAccess() const { return lastAccess; }
};


//----------------------------------------------------------------------------
// TableI
//----------------------------------------------------------------------------
class kdi::net::details::TableI
    : virtual public kdi::net::details::Table,
      private boost::noncopyable
{
    kdi::TablePtr table;

    kdi::Timestamp lastAccess;
    TimeoutLocatorPtr locator;
    
public:
    TableI(kdi::TablePtr const & table,
           TimeoutLocatorPtr const & locator);
    ~TableI();

    virtual void applyMutations(Ice::ByteSeq const & cells,
                                Ice::Current const & cur);

    virtual void sync(Ice::Current const & cur);

    virtual ScannerPrx scan(std::string const & predicate,
                            Ice::Current const & cur);

    kdi::Timestamp const & getLastAccess() const { return lastAccess; }
};


//----------------------------------------------------------------------------
// TableManagerI
//----------------------------------------------------------------------------
class kdi::net::details::TableManagerI
    : virtual public kdi::net::details::TableManager,
      private boost::noncopyable
{
    TimeoutLocatorPtr locator;

public:
    explicit TableManagerI(TimeoutLocatorPtr const & locator);
    ~TableManagerI();

    virtual TablePrx openTable(std::string const & path,
                               Ice::Current const & cur);
};


#endif // KDI_NET_TABLEMANAGERI_H
