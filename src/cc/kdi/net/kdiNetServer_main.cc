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

// For LocalTable implementation
#include <kdi/local/local_table.h>

// For Tablet implementation
#include <kdi/tablet/Tablet.h>
#include <kdi/tablet/SharedLogger.h>
#include <kdi/tablet/SharedCompactor.h>
#include <kdi/tablet/FileConfigManager.h>

// For SuperTablet implementation
#include <kdi/tablet/MetaConfigManager.h>
#include <kdi/tablet/SuperTablet.h>

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
        // XXX: strangeness.  The fixed tables will need to load their
        // configs out of a file like object.  All other tablets will
        // have configs in the meta table.  The other config manager
        // functions are common, I think.  New files and such will be
        // generated in the normal way.  They will still share log
        // files.  However, tables from a file config cannot be split.
        // So we wind up with something like this:
        //
        // class CommonConfigManager : public ConfigManager { ... };
        // class FileConfigManager : public ConfigManager {
        //    CommonConfigManagerPtr common;
        //    ...(file config)...
        // };
        // class MetaConfigManager : public ConfigManager {
        //    CommonConfigManagerPtr common;
        //    ...(meta config)...
        // };
        //
        // The CommonConfigManager need not actually be a
        // ConfigManager.  It can just hold the common state.
      

        tablet::MetaConfigManagerPtr metaConfigMgr;
        tablet::SharedLoggerSyncPtr logger;
        tablet::SharedCompactorPtr compactor;

        tablet::ConfigManagerPtr fixedConfigMgr;
        std::vector<std::string> fixedTables;

    public:
        SuperTabletMaker(std::string const & root,
                         std::string const & metaTable,
                         std::string const & server,
                         std::vector<std::string> const & fixedTables_) :
            metaConfigMgr(new tablet::MetaConfigManager(root, server)),
            logger(new Synchronized<tablet::SharedLogger>(metaConfigMgr)),
            compactor(new tablet::SharedCompactor),
            fixedTables(fixedTables_)
        {
            log("SuperTabletMaker %p: created", this);

            if(!fixedTables.empty())
            {
                fixedConfigMgr = metaConfigMgr->getFixedAdapter();
                std::sort(fixedTables.begin(), fixedTables.end());
            }

            log("Loading from META table: %s", metaTable);
            metaConfigMgr->loadMeta(metaTable);
        }
        
        ~SuperTabletMaker()
        {
            compactor->shutdown();
            LockedPtr<tablet::SharedLogger>(*logger)->shutdown();

            log("SuperTabletMaker %p: destroyed", this);
        }

        TablePtr makeTable(std::string const & name) const
        {
            if(std::binary_search(fixedTables.begin(), fixedTables.end(), name))
            {
                log("Load fixed table: %s", name);
                TablePtr p(
                    new tablet::Tablet(
                        name, fixedConfigMgr, logger, compactor
                        )
                    );
                return p;
            }
            else
            {
                log("Load table: %s", name);
                TablePtr p(
                    new tablet::SuperTablet(
                        name, metaConfigMgr, logger, compactor
                        )
                    );
                return p;
            }


            // XXX : scan meta table ... or... not sure yet.  This
            // gets called if a client has requested a table we don't
            // know about.

            // in some cases, that means the meta table has been
            // updated.  but there's also the case of a request for an
            // invalid table (not in the meta table).

            // not sure yet how to connect tablets to clients.

        }
    };

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
                op.addOption("mode,m", value<string>()->default_value("local"),
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
                
                // The usual procedure for loading a table is to scan
                // the META table for config information and load the
                // tablets that are hosted by this server.  This won't
                // work for loading the META table.  The server
                // hosting the META table must load it separately.
                op.addOption("load,l", value< vector<string> >(),
                             "Preload a table and pin it to the server");
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
            if(mode == "local")
            {
                log("Starting in LocalTable mode");
                boost::shared_ptr<LocalTableMaker> p(new LocalTableMaker(tableRoot));
                tableMaker = boost::bind(&LocalTableMaker::makeTable, p, _1);
            }
            else if(mode == "tablet")
            {
                log("Starting in Tablet mode");
                boost::shared_ptr<TabletMaker> p(new TabletMaker(tableRoot));
                tableMaker = boost::bind(&TabletMaker::makeTable, p, _1);
            }
            else if(mode == "super")
            {
                string meta;
                if(!opt.get("meta", meta) || meta.empty())
                    op.error("need --meta");

                string server;
                if(!opt.get("server", server) || server.empty())
                    op.error("need --server");

                vector<string> load;
                opt.get("load", load);

                log("Starting in SuperTablet mode");
                boost::shared_ptr<SuperTabletMaker> p(
                    new SuperTabletMaker(tableRoot, meta, server, load)
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
