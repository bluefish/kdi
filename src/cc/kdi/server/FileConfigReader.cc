//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-06-11
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

#include <kdi/server/FileConfigReader.h>
#include <kdi/server/FileConfigWriter.h>
#include <kdi/server/TabletConfig.h>
#include <kdi/server/name_util.h>
#include <warp/fs.h>
#include <warp/config.h>
#include <warp/dir.h>
#include <ex/exception.h>

using namespace kdi::server;
using namespace warp;
using std::string;
using namespace ex;

namespace {

    BoundType getBoundType(Config const & x, std::string const & name)
    {
        string t = x.get(name);
        if(t == "infinite")
            return BT_INFINITE;
        else if(t == "inclusive")
            return BT_INCLUSIVE;
        else if(t == "exclusive")
            return BT_EXCLUSIVE;
        else
            raise<RuntimeError>("unknown bound type: %s", t);
    }

    TabletConfigCPtr readConfigFile(std::string const & fn)
    {
        Config x(fn);
        TabletConfigPtr config(new TabletConfig);
    
        config->tableName = x.get("table");
        config->rows.setLowerBound(
            x.get("min.value", ""), getBoundType(x, "min.type"));
        config->rows.setUpperBound(
            x.get("max.value", ""), getBoundType(x, "max.type"));
        config->log = x.get("log", "");
        config->location = x.get("location", "");

        Config const * frag = x.findChild("frag");
        size_t nFrags = frag ? frag->numChildren() : 0;
        config->fragments.resize(nFrags);
        for(size_t i = 0; i < nFrags; ++i)
        {
            Config const & fi = frag->getChild(i);
            Config const * family = fi.findChild("family");

            config->fragments[i].filename = fi.get("filename");
        
            size_t nFamilies = family ? family->numChildren() : 0;
            config->fragments[i].families.resize(nFamilies);
            for(size_t j = 0; j < nFamilies; ++j)
                config->fragments[i].families[j] = family->getChild(j).get();
        }

        return config;
    }

    std::string readConfigName(std::string const & fn)
    {
        string tabletName;

        Config x(fn);
        string tableName = x.get("table");
        Interval<string> rows;
        rows.setUpperBound(
            x.get("max.value", ""), getBoundType(x, "max.type"));
        
        encodeTabletName(tableName, rows.getUpperBound(), tabletName);

        return tabletName;
    }

}

//----------------------------------------------------------------------------
// FileConfigReader
//----------------------------------------------------------------------------
FileConfigReader::FileConfigReader(std::string const & configDir) :
    configDir(configDir)
{
}

TabletConfigCPtr FileConfigReader::readConfig(std::string const & tabletName)
{
    return readConfigFile(
        fs::resolve(
            configDir,
            FileConfigWriter::getConfigFilename(tabletName)));
}

std::vector<TabletConfigCPtr> FileConfigReader::readAllConfigs()
{
    std::vector<TabletConfigCPtr> cfgs;
    DirPtr dp = Directory::open(configDir);
    for(std::string fn; dp->readPath(fn); )
        cfgs.push_back(readConfigFile(fn));
    dp->close();
    return cfgs;
}

std::vector<std::string> FileConfigReader::readAllNames()
{
    std::vector<std::string> names;
    DirPtr dp = Directory::open(configDir);
    for(std::string fn; dp->readPath(fn); )
        names.push_back(readConfigName(fn));
    dp->close();
    return names;
}
