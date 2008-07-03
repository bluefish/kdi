//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-08-20
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

#include "recordstream.h"
#include "warp/uri.h"

using namespace oort;
using namespace warp;
using namespace ex;
using namespace std;

namespace 
{
#if 0
}
#endif

struct Checker : public RecordStream
{
    size_t nRecs;
    uint32_t type;
    uint32_t ver;
    uint32_t minLen;
    uint32_t maxLen;
    bool checkType;
    bool checkVersion;
    bool checkMinLength;
    bool checkMaxLength;

    RecordStreamHandle s;

    Checker() :
        nRecs(0),
        checkType(false),
        checkVersion(false),
        checkMinLength(false),
        checkMaxLength(false)
    {
    }

    void check(Record const & x)
    {
        ++nRecs;
        try {
            if(checkType && x.getType() != type)
                raise<RuntimeError>("type");
            if(checkVersion && x.getVersion() != ver)
                raise<RuntimeError>("version");
            if(checkMinLength && x.getLength() < minLen)
                raise<RuntimeError>("length");
            if(checkMaxLength && x.getLength() > maxLen)
                raise<RuntimeError>("length");
        }
        catch(RuntimeError const & ex) {
            raise<RuntimeError>("record %1% %2% mismatch: %3%",
                                nRecs, ex.what(), getName());
        }
    }

    void put(Record const & x)
    {
        check(x);
        s->put(x);
    }

    bool get(Record & x)
    {
        if(s->get(x))
        {
            check(x);
            return true;
        }
        else
            return false;
    }

    string getName() const
    {
        if(s)
            return "check:"+s->getName();
        else
            return "check";
    }
};

RecordStreamHandle openCheckStream(string const & uriFn, int flags)
{
    Uri uri(wrap(uriFn));

    Checker * c = new Checker();
    RecordStreamHandle h(c);

    for(UriQuery q(uri.query); q.hasAttr(); q.next())
    {
        if(q.key == "type")
        {
            if(q.value.size() != 4)
                raise<RuntimeError>("need type of length 4");
            char const * t = q.value.begin();
            c->checkType = true;
            c->type = OORT_TYPE(t[0], t[1], t[2], t[3]);
        }
        else if(q.key == "ver")
        {
            c->checkVersion = true;
            c->ver = parseInt<uint32_t>(q.value);
        }
        else if(q.key == "minlen")
        {
            c->checkMinLength = true;
            c->minLen = parseInt<uint32_t>(q.value);
        }
        else if(q.key == "maxlen")
        {
            c->checkMaxLength = true;
            c->maxLen = parseInt<uint32_t>(q.value);
        }
        else if(q.key == "len")
        {
            c->checkMinLength = c->checkMaxLength = true;
            c->minLen = c->maxLen = parseInt<uint32_t>(q.value);
        }
    }

    c->s = openStream(str(uri.popScheme()), flags);

    return h;
}

#if 0
namespace 
{
#endif
}

#include "warp/init.h"
WARP_DEFINE_INIT(oort_checkstream)
{
    registerScheme("check", &openCheckStream);
}
