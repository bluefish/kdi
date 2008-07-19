//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/TabletName.cc $
//
// Created 2008/07/17
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/tablet/TabletName.h>
#include <warp/strutil.h>
#include <warp/tuple_encode.h>
#include <ex/exception.h>

using namespace kdi::tablet;
using namespace warp;
using namespace ex;
using namespace std;

TabletName::TabletName(std::string const & encodedName)
{
    string sep, last;
    decodeTuple(encodedName, tie(tableName, sep, last));

    if(sep == "\x01")
        lastRow = IntervalPoint<string>(last, PT_INCLUSIVE_UPPER_BOUND);
    else if(sep == "\x02")
        lastRow = IntervalPoint<string>(string(), PT_INFINITE_UPPER_BOUND);
    else
        raise<ValueError>("invalid encoded tablet name: %s",
                          reprString(encodedName));
}

TabletName::TabletName(std::string const & tableName,
                       warp::IntervalPoint<std::string> lastRow) :
    tableName(tableName),
    lastRow(lastRow)
{
    if(tableName.empty())
        raise<ValueError>("tableName cannot be empty");

    switch(lastRow.getType())
    {
        case PT_INFINITE_UPPER_BOUND:
        case PT_INCLUSIVE_UPPER_BOUND:
            break;

        default:
            raise<ValueError>("lastRow must be inclusive or infinite "
                              "upper bound");
    }
}

std::string TabletName::getEncoded() const
{
    if(lastRow.isFinite())
        return encodeTuple(make_tuple(tableName, "\x01", lastRow.getValue()));
    else
        return encodeTuple(make_tuple(tableName, "\x02", ""));
}
