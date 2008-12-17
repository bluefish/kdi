//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-10
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

#include <kdi/net/TableManagerI.h>
#include <kdi/net/TimeoutLocator.h>
#include <warp/options.h>
#include <warp/fs.h>
#include <warp/log.h>
#include <ex/exception.h>
#include <Ice/Ice.h>
#include <boost/bind.hpp>
#include <iostream>
#include <vector>
#include <string>

// For SuperTablet implementation
#include <kdi/tablet/SuperTablet.h>
#include <kdi/tablet/Tablet.h>
#include <kdi/tablet/TabletConfig.h>
#include <kdi/tablet/SharedLogger.h>
#include <kdi/tablet/SharedCompactor.h>
#include <kdi/tablet/WorkQueue.h>
#include <kdi/tablet/FileTracker.h>
#include <kdi/tablet/MetaConfigManager.h>
#include <warp/tuple_encode.h>

// For getHostName
#include <unistd.h>
#include <cstring>

using namespace kdi;
using namespace kdi::net;
using namespace warp;
using namespace std;
using namespace ex;

namespace {

    class SuperTabletServer
    {
        tablet::MetaConfigManagerPtr metaConfigMgr;
        tablet::FileTrackerPtr tracker;
        tablet::SharedLoggerPtr logger;
        tablet::SharedCompactorPtr compactor;
        tablet::WorkQueuePtr workQueue;
        TablePtr metaTable;
        std::string server;

    public:
        SuperTabletServer(std::string const & root,
                         std::string const & server) :
            metaConfigMgr(new tablet::MetaConfigManager(root, server)),
            tracker(new tablet::FileTracker),
            logger(new tablet::SharedLogger(metaConfigMgr, tracker)),
            compactor(new tablet::SharedCompactor),
            workQueue(new tablet::WorkQueue(1)),
            server(server)
        {
            log("SuperTabletServer %p: created", this);

            log("Creating META table");

            tablet::ConfigManagerPtr fixedMgr =
                metaConfigMgr->getFixedAdapter();
            std::list<tablet::TabletConfig> cfgs =
                fixedMgr->loadTabletConfigs("META");
            if(cfgs.size() != 1)
                raise<RuntimeError>("loaded %d configs for META table",
                                    cfgs.size());

            metaTable = tablet::Tablet::make(
                "META",
                fixedMgr,
                logger,
                compactor,
                tracker,
                workQueue,
                cfgs.front());

            metaConfigMgr->setMetaTable(metaTable);
        }

        ~SuperTabletServer()
        {
            log("SuperTabletServer %p: destroyed", this);
        }

        TablePtr makeTable(std::string const & name) const
        {
            if(metaTable && name == "META")
            {
                log("Load META table");
                return metaTable;
            }
            else
            {
                try {
                    log("Load table: %s", name);
                    TablePtr p(
                        new tablet::SuperTablet(
                            name, metaConfigMgr, logger,
                            compactor, tracker, workQueue
                            )
                        );
                    return p;
                }
                catch(kdi::tablet::TableDoesNotExistError const & ex) {
                    log("Create table: %s", name);
                    metaTable->set(
                        encodeTuple(make_tuple(name, "\x02", "")),
                        "config",
                        0,
                        "server = " + server + "\n");
                    metaTable->sync();

                    log("Load table again: %s", name);
                    TablePtr p(
                        new tablet::SuperTablet(
                            name, metaConfigMgr, logger,
                            compactor, tracker, workQueue
                            )
                        );
                    return p;
                }
            }
        }

        void shutdown()
        {
            compactor->shutdown();
            workQueue->shutdown();
            logger->shutdown();
        }
    };

    std::string getHostName()
    {
        char buf[256];
        if(0 == gethostname(buf, sizeof(buf)) && strcmp(buf, "localhost"))
            return std::string(buf);
        else
            return std::string();
    }

    class ServerApp
        : public virtual Ice::Application
    {
    public:
        void appMain(int ac, char ** av)
        {
            // Set options
            OptionParser op("%prog [ICE-parameters] [options]");
            {
                using namespace boost::program_options;
                op.addOption("root,r", value<string>(),
                             "Root directory for tablet data");
            }

            // Parse options
            OptionMap opt;
            ArgumentList args;
            op.parse(ac, av, opt, args);

            // Get table root directory
            string tableRoot;
            if(!opt.get("root", tableRoot))
                op.error("need --root");

            // Create table server
            boost::shared_ptr<SuperTabletServer> server(
                new SuperTabletServer(
                    tableRoot, getHostName()));

            // Create adapter
            Ice::CommunicatorPtr ic = communicator();
            Ice::ObjectAdapterPtr adapter
                = ic->createObjectAdapter("TableAdapter");

            // Create locator
            TimeoutLocatorPtr locator = new TimeoutLocator(
                boost::bind(
                    &SuperTabletServer::makeTable,
                    server, _1));
            adapter->addServantLocator(locator, "");

            // Create TableManager object
            Ice::ObjectPtr object = new ::kdi::net::details::TableManagerI(locator);
            adapter->add(object, ic->stringToIdentity("TableManager"));

            // Run server
            adapter->activate();
            ic->waitForShutdown();

            // Shutdown
            log("Shutting down");
            server->shutdown();

            // Destructors after this point
            log("Cleaning up");
        }

        virtual int run(int ac, char ** av)
        {
            try {
                appMain(ac, av);
            }
            catch(OptionError const & err) {
                cerr << err << endl;
                return 2;
            }
            catch(Exception const & err) {
                cerr << err << endl
                     << err.getBacktrace();
                return 1;
            }
            return 0;
        }
    };

}

int main(int ac, char ** av)
{
    ServerApp app;
    return app.main(ac, av);
}
