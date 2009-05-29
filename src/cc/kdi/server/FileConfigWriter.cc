//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-28
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

#include <kdi/server/FileConfigWriter.h>
#include <warp/md5.h>
#include <warp/strutil.h>
#include <warp/tuple.h>
#include <warp/config.h>
#include <warp/fs.h>
#include <warp/file.h>
#include <sstream>

#include <boost/format.hpp>

using namespace kdi;
using namespace kdi::server;
using namespace warp;
using boost::format;

namespace {

    std::string getConfigFilename(TabletConfig const & cfg)
    {
        std::ostringstream last;
        last << cfg.rows.getUpperBound();
        
        std::ostringstream fn;
        fn << replace(cfg.tableName, "/", ".");
        fn << '.';
        fn << md5HexDigest(last.str());

        return fn.str();
    }

    void setBound(IntervalPoint<std::string> const & pt,
                  std::string const & name,
                  Config & out)
    {
        out.set(name + ".type",
                pt.isInfinite() ? "infinite" :
                pt.isInclusive() ? "inclusive" :
                "exclusive");
        if(pt.isFinite())
            out.set(name + ".value", pt.getValue());
    }

}

//----------------------------------------------------------------------------
// FileConfigWriter
//----------------------------------------------------------------------------
FileConfigWriter::FileConfigWriter(std::string const & configDir) :
    configDir(configDir)
{
    fs::makedirs(configDir);
}

void FileConfigWriter::writeConfigs(std::vector<TabletConfig> const & configs)
{
    for(std::vector<TabletConfig>::const_iterator i = configs.begin();
        i != configs.end(); ++i)
    {
        writeConfig(*i);
    }
}

void FileConfigWriter::writeConfig(TabletConfig const & cfg)
{
    std::string fn = getConfigFilename(cfg);
    std::string outPath = fs::resolve(configDir, fn);
    std::string tmpPath = fs::resolve(configDir, fn + "-$(UNIQUE)");

    FilePtr out;
    tie(out, tmpPath) = File::openUnique(tmpPath);

    Config x;
    x.set("table", cfg.tableName);
    setBound(cfg.rows.getLowerBound(), "min", x);
    setBound(cfg.rows.getUpperBound(), "max", x);
    x.set("log", cfg.log);
    x.set("location", cfg.location);
    
    for(size_t i = 0; i < cfg.fragments.size(); ++i)
    {
        x.set(str(format("frag.i%d.filename") % i),
              cfg.fragments[i].filename);

        for(size_t j = 0; j < cfg.fragments[i].families.size(); ++j)
        {
            x.set(str(format("frag.i%d.column.i%d") % i % j),
                  cfg.fragments[i].families[j]);
        }
    }  

    x.writeProperties(out);
    out->close();
    
    fs::rename(tmpPath, outPath, true);
}
