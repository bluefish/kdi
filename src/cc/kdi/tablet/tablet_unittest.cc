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
        SharedLoggerSyncPtr syncLogger;
        SharedCompactorPtr compactor;

        Setup() :
            configMgr(new FileConfigManager("memfs:/")),
            syncLogger(new Synchronized<SharedLogger>(configMgr)),
            compactor(new SharedCompactor)
        {
        }
    };
}


BOOST_AUTO_UNIT_TEST(table_api_basic)
{
    Setup s;
    TabletPtr t(new Tablet("foo", s.configMgr, s.syncLogger, s.compactor));

    unittest::testTableInterface(t);
}
