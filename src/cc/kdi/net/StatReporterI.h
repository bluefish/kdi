//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-30
//
// This file is part of KDI.
//
// KDI is free software; you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the Free Software
// Foundation; either version 2 of the License, or any later version.
//
// KDI is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
// details.
//
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef KDI_NET_STATREPORTERI_H
#define KDI_NET_STATREPORTERI_H

#include <Ice/Ice.h>
#include <string>
#include <map>
#include <stdint.h>

namespace kdi {
namespace net {

    class StatReportable
    {
    public:
        typedef std::map<std::string, int64_t> StatMap;

        virtual void getStats(StatMap & stats) const = 0;

    protected:
        ~StatReportable() {}
    };

    Ice::ObjectPtr makeStatReporter(StatReportable * reportable);

} // namespace net
} // namespace kdi

#endif // KDI_NET_STATREPORTERI_H
