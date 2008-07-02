//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/tablet_unittest.cc $
//
// Created 2008/05/19
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
