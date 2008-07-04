//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-03-27
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

#include <oort/record.h>
#include <warp/strutil.h>
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
