//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-19
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

#include <unittest/main.h>
#include <kdi/tablet/Tablet.h>
#include <kdi/tablet/TabletConfig.h>
#include <kdi/table_unittest.h>

#include <kdi/tablet/SharedLogger.h>
#include <kdi/tablet/SharedCompactor.h>
#include <kdi/tablet/FileConfigManager.h>
#include <boost/format.hpp>

#include <warp/init.h>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace ex;
using namespace std;
using boost::format;

namespace
{
    struct ModuleInit
    {
        ModuleInit()
        {
            warp::initModule();
        }
    };

    struct Setup : private ModuleInit
    {
        ConfigManagerPtr configMgr;
        SharedLoggerPtr logger;
        SharedCompactorPtr compactor;

        Setup() :
            configMgr(new FileConfigManager("memfs:/")),
            logger(new SharedLogger(configMgr)),
            compactor(new SharedCompactor)
        {
        }
    };
}


BOOST_AUTO_UNIT_TEST(table_api_basic)
{
    std::string const NAME("foo");

    Setup s;
    std::list<TabletConfig> cfgs = s.configMgr->loadTabletConfigs(NAME);

    for(std::list<TabletConfig>::const_iterator i = cfgs.begin();
        i != cfgs.end(); ++i)
    {
        TabletPtr t(
            new Tablet(NAME, s.configMgr, s.logger, s.compactor, *i));

        unittest::testTableInterface(t);
    }
}
