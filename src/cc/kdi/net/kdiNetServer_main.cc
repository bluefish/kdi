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

// For LocalTable implementation
#include <kdi/local/local_table.h>

// For Tablet implementation
#include <kdi/tablet/Tablet.h>
#include <kdi/tablet/SharedLogger.h>
#include <kdi/tablet/SharedCompactor.h>
#include <kdi/tablet/FileConfigManager.h>

using namespace kdi;
using namespace kdi::net;
using namespace warp;
using namespace std;
using namespace ex;

namespace {

    class TabletMaker
    {
        tablet::ConfigManagerPtr configMgr;
        tablet::SharedLoggerSyncPtr logger;
        tablet::SharedCompactorPtr compactor;

    public:
        explicit TabletMaker(std::string const & root) :
            configMgr(new tablet::FileConfigManager(root)),
            logger(new Synchronized<tablet::SharedLogger>(configMgr)),
            compactor(new tablet::SharedCompactor)
        {
            log("TabletMaker %p: created", this);
        }
        
        ~TabletMaker()
        {
            compactor->shutdown();
            LockedPtr<tablet::SharedLogger>(*logger)->shutdown();

            log("TabletMaker %p: destroyed", this);
        }

        TablePtr makeTable(std::string const & name) const
        {
            TablePtr p(
                new tablet::Tablet(
                    name, configMgr, logger, compactor
                    )
                );
            return p;
        }
    };

    class LocalTableMaker
    {
        std::string root;

    public:
        explicit LocalTableMaker(std::string const & root) :
            root(root)
        {
        }

        TablePtr makeTable(std::string const & name) const
        {
            TablePtr p(
                new local::LocalTable(
                    fs::resolve(root, name)
                    )
                );
            return p;
        }
    };

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
                op.addOption("root,r", value<string>()->default_value("."),
                             "Server root directory");
                op.addOption("tablet,t", "Tablet server mode");
            }

            // Parse options
            OptionMap opt;
            ArgumentList args;
            op.parse(ac, av, opt, args);

            // Get table root directory
            string tableRoot;
            if(!opt.get("root", tableRoot))
                op.error("need --root");

            // Set up table maker
            boost::function<TablePtr (std::string const &)> tableMaker;
            if(hasopt(opt, "tablet"))
            {
                log("Starting in Tablet mode");
                boost::shared_ptr<TabletMaker> p(new TabletMaker(tableRoot));
                tableMaker = boost::bind(&TabletMaker::makeTable, p, _1);
            }
            else
            {
                log("Starting in LocalTable mode");
                boost::shared_ptr<LocalTableMaker> p(new LocalTableMaker(tableRoot));
                tableMaker = boost::bind(&LocalTableMaker::makeTable, p, _1);
            }

            // Create adapter
            Ice::CommunicatorPtr ic = communicator();
            Ice::ObjectAdapterPtr adapter
                = ic->createObjectAdapter("TableAdapter");

            // Create locator
            TimeoutLocatorPtr locator = new TimeoutLocator(tableMaker);
            adapter->addServantLocator(locator, "");

            // Create TableManager object
            Ice::ObjectPtr object = new details::TableManagerI(locator);
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
