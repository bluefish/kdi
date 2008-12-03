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

    class SuperTabletMaker
    {
        tablet::MetaConfigManagerPtr metaConfigMgr;
        tablet::FileTrackerPtr tracker;
        tablet::SharedLoggerPtr logger;
        tablet::SharedCompactorPtr compactor;
        tablet::WorkQueuePtr workQueue;
        TablePtr metaTable;
        std::string server;

    public:
        SuperTabletMaker(std::string const & root,
                         std::string const & metaTableUri,
                         std::string const & server) :
            metaConfigMgr(new tablet::MetaConfigManager(root, server)),
            tracker(new tablet::FileTracker),
            logger(new tablet::SharedLogger(metaConfigMgr, tracker)),
            compactor(new tablet::SharedCompactor),
            workQueue(new tablet::WorkQueue(1)),
            server(server)
        {
            log("SuperTabletMaker %p: created", this);

            if(metaTableUri.empty())
            {
                log("Creating META table");

                tablet::ConfigManagerPtr fixedMgr =
                    metaConfigMgr->getFixedAdapter();
                std::list<tablet::TabletConfig> cfgs =
                    fixedMgr->loadTabletConfigs("META");
                if(cfgs.size() != 1)
                    raise<RuntimeError>("loaded %d configs for META table",
                                        cfgs.size());

                tablet::TabletPtr p(
                    new tablet::Tablet(
                        "META",
                        fixedMgr,
                        logger,
                        compactor,
                        tracker,
                        workQueue,
                        cfgs.front()
                        )
                    );
                metaTable = p;

                // XXX hack
                compactor->requestCompaction(p);
            }
            else
            {
                log("Connecting to META table: %s", metaTableUri);
                metaTable = Table::open(metaTableUri);
            }

            metaConfigMgr->setMetaTable(metaTable);
        }
        
        ~SuperTabletMaker()
        {
            compactor->shutdown();
            workQueue->shutdown();
            logger->shutdown();

            log("SuperTabletMaker %p: destroyed", this);
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
                op.addOption("mode,m", value<string>()->default_value("super"),
                             "Server mode");

                op.addOption("root,r", value<string>()->default_value("."),
                             "Root directory for tablet data");

                // This option tells the super tablet server where to
                // find the meta table for getting table config
                // information.  It is fine for the server that hosts
                // the META table to refer to itself.  It does not
                // imply that the table should be loaded.


                // Examples: --meta=kdi://host:port/META
                //           --meta=dref://ls-host:port/some/node
                op.addOption("meta,M", value<string>(),
                             "Location of META table");

                op.addOption("server,s",
                             value<string>()->default_value(getHostName()),
                             "Name of server");
            }

            // Parse options
            OptionMap opt;
            ArgumentList args;
            op.parse(ac, av, opt, args);

            // Get the server mode
            string mode;
            if(!opt.get("mode", mode))
                op.error("need --mode");

            // Get table root directory
            string tableRoot;
            if(!opt.get("root", tableRoot))
                op.error("need --root");


            // Init server based on mode
            boost::function<TablePtr (std::string const &)> tableMaker;
            if(mode == "super")
            {
                string meta;
                opt.get("meta", meta);

                string server;
                if(!opt.get("server", server) || server.empty())
                    op.error("need --server");

                log("Starting in SuperTablet mode");
                boost::shared_ptr<SuperTabletMaker> p(
                    new SuperTabletMaker(tableRoot, meta, server)
                    );
                tableMaker = boost::bind(&SuperTabletMaker::makeTable, p, _1);
            }
            else
                op.error("unknown --mode: " + mode);

            // Create adapter
            Ice::CommunicatorPtr ic = communicator();
            Ice::ObjectAdapterPtr adapter
                = ic->createObjectAdapter("TableAdapter");

            // Create locator
            TimeoutLocatorPtr locator = new TimeoutLocator(tableMaker);
            adapter->addServantLocator(locator, "");

            // Create TableManager object
            Ice::ObjectPtr object = new ::kdi::net::details::TableManagerI(locator);
            adapter->add(object, ic->stringToIdentity("TableManager"));

            // Run server
            adapter->activate();
            ic->waitForShutdown();

            log("Shutting down");
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
