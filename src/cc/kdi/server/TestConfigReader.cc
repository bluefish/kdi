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

void TestConfigReader::readSchemas_async(
    ReadSchemasCb * cb,
    std::vector<std::string> const & tableNames)
{
    std::vector<TableSchema> schemas(tableNames.size());
    for(size_t i = 0; i < tableNames.size(); ++i)
    {
        schemas[i].tableName = tableNames[i];
        schemas[i].groups.resize(groups.size());
        for(size_t j = 0; j < groups.size(); ++j)
        {
            schemas[i].groups[j].columns = groups[j];
        }
    }
    cb->done(schemas);
}
        
void TestConfigReader::readConfigs_async(
    ReadConfigsCb * cb,
    std::vector<std::string> const & tabletNames)
{
    std::vector<TabletConfig> configs(tabletNames.size());
    for(size_t i = 0; i < tabletNames.size(); ++i)
    {
        warp::IntervalPoint<std::string> last;
        decodeTabletName(tabletNames[i], configs[i].tableName, last);
        configs[i].rows.unsetLowerBound().setUpperBound(last);
    }
    cb->done(configs);
}

