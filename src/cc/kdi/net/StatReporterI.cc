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

#include <kdi/net/StatReporter.h>
#include <kdi/net/StatReporterI.h>
#include <ex/exception.h>

using namespace kdi::net::details;
using kdi::net::StatReportable;

namespace {

    class StatReporterI : public StatReporter
    {
        StatReportable * stats;

    public:
        StatReporterI(StatReportable * stats) : stats(stats) {}

        void getStats(StatMap & map, Ice::Current const & cur)
        {
            stats->getStats(map);
        }
    };
}

Ice::ObjectPtr kdi::net::makeStatReporter(StatReportable * reportable)
{
    EX_CHECK_NULL(reportable);
    Ice::ObjectPtr p = new StatReporterI(reportable);
    return p;
}
