//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/oort/record.cc#1 $
//
// Created 2006/03/27
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "record.h"
#include "warp/strutil.h"
#include <boost/format.hpp>
#include <boost/scoped_array.hpp>

using namespace oort;
using namespace warp;
using namespace std;

//----------------------------------------------------------------------------
// Record
//----------------------------------------------------------------------------
string Record::getTypeStr() const
{
    return typeString(getType());
}

string Record::toString() const
{
    return boost::str(boost::format("<Record '%1%' v%2% len=%3%>")
                      % getTypeStr() % getVersion() % getLength());
}

namespace
{
    class SimpleStore : public RecordStore
    {
        HeaderSpec::Fields hdr;
        boost::scoped_array<char> data;

    public:
        explicit SimpleStore(Record const & r) :
            hdr(r), data(new char[hdr.length])
        {
            memcpy(data.get(), r.getData(), hdr.length);
        }

        uint32_t     getLength(char const *) const { return hdr.length; }
        uint32_t     getType(char const *) const { return hdr.type; }
        uint32_t     getFlags(char const *) const { return hdr.flags; }
        uint32_t     getVersion(char const *) const { return hdr.version; }
        char const * getData(char const *) const { return data.get(); }
    };
}

Record Record::clone() const
{
    if(isNull())
        return Record();
    else
    {
        SimpleStore * store = new SimpleStore(*this);
        Record r(store, 0);
        store->release();
        return r;
    }
}
