//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-07
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

#include <kdi/server/TestConfigReader.h>
#include <kdi/server/TabletConfig.h>
#include <kdi/server/TableSchema.h>
#include <kdi/server/name_util.h>

using namespace kdi;
using namespace kdi::server;

//----------------------------------------------------------------------------
// TestConfigReader
//----------------------------------------------------------------------------
TestConfigReader::TestConfigReader()
{
    groups.resize(1);
    groups[0].push_back("test");
}

TestConfigReader::TestConfigReader(char const * const * familyGroups)
{
    while(*familyGroups)
    {
        groups.resize(groups.size() + 1);
        std::vector<std::string> & families = groups.back();
        while(*familyGroups)
        {
            families.push_back(*familyGroups);
            ++familyGroups;
        }
    }
}

TableSchemaCPtr TestConfigReader::readSchema(std::string const & tableName)
{
    TableSchemaPtr p(new TableSchema);
    p->tableName = tableName;
    p->groups.resize(groups.size());
    for(size_t i = 0; i < groups.size(); ++i)
    {
        p->groups[i].families = groups[i];
    }
    return p;
}

TabletConfigCPtr TestConfigReader::readConfig(std::string const & tabletName)
{
    TabletConfigPtr p(new TabletConfig);
    warp::IntervalPoint<std::string> last;
    decodeTabletName(tabletName, p->tableName, last);
    p->rows.unsetLowerBound().setUpperBound(last);
    return p;
}
