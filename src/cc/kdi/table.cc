//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/table.cc#2 $
//
// Created 2007/09/12
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/table.h>
#include <kdi/scan_predicate.h>
#include <kdi/table_factory.h>
#include <kdi/cell_filter.h>

using namespace kdi;

//----------------------------------------------------------------------------
// Table
//----------------------------------------------------------------------------
void Table::insert(Cell const & x)
{
    if(x.isErasure())
        erase(x.getRow(), x.getColumn(), x.getTimestamp());
    else
        set(x.getRow(), x.getColumn(), x.getTimestamp(), x.getValue());
}

CellStreamPtr Table::scan(ScanPredicate const & pred) const
{
    return applyPredicateFilter(pred, scan());
}

CellStreamPtr Table::scan(strref_t predExpr) const
{
    return scan(ScanPredicate(predExpr));
}

TablePtr Table::open(std::string const & uri)
{
    return TableFactory::get().create(uri);
}
