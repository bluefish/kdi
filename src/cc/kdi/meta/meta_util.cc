//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/meta/meta_util.cc $
//
// Created 2008/03/19
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/meta/meta_util.h>
#include <warp/tuple_encode.h>
#include <sstream>
#include <boost/format.hpp>

using namespace kdi;
using namespace kdi::meta;
using namespace warp;
using namespace std;
using boost::format;

string kdi::meta::encodeMetaRow(strref_t tableName, strref_t row)
{
    return encodeTuple(make_tuple(tableName, "\x01", row));
}

string kdi::meta::encodeLastMetaRow(strref_t tableName)
{
    return encodeTuple(make_tuple(tableName, "\x02", ""));
}

CellStreamPtr kdi::meta::metaLocationScan(
    TablePtr const & metaTable, strref_t tableName, strref_t row)
{
    // Build our query -- we want a scan that starts at the meta
    // table row containing the given row

    ostringstream oss;
    oss << format("%s <= row <= %s and column = 'location' and history = 1")
        % reprString(wrap(encodeMetaRow(tableName, row)))
        % reprString(wrap(encodeLastMetaRow(tableName)))
        ;

    return metaTable->scan(oss.str());
}

IntervalPoint<string> kdi::meta::getTabletRowBound(strref_t metaRow)
{
    string tableName, sep, lastRow;
    decodeTuple(metaRow, tie(tableName, sep, lastRow));
    
    if(sep == "\x01")
        return IntervalPoint<string>(lastRow, PT_INCLUSIVE_UPPER_BOUND);
    else
        return IntervalPoint<string>("", PT_INFINITE_UPPER_BOUND);
}
