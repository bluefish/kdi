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
#include <kdi/net/TableLocator.h>
#include <kdi/net/ScannerLocator.h>
#include <kdi/net/StatReporterI.h>
#include <warp/options.h>
#include <warp/fs.h>
#include <warp/filestream.h>
#include <warp/log.h>
#include <ex/exception.h>
#include <Ice/Ice.h>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <warp/call_or_die.h>
#include <iostream>
#include <vector>
#include <string>
#include <cstdlib>
#include <new>

// For SuperTablet implementation
#include <kdi/tablet/SuperTablet.h>
#include <kdi/tablet/Tablet.h>
#include <kdi/tablet/TabletConfig.h>
#include <kdi/tablet/SharedLogger.h>
#include <kdi/tablet/SharedCompactor.h>
#include <kdi/tablet/WorkQueue.h>
#include <kdi/tablet/FileTracker.h>
#include <kdi/tablet/MetaConfigManager.h>
#include <kdi/tablet/TabletGc.h>
#include <warp/tuple_encode.h>

#include <kdi/local/index_cache.h>

// For getHostName
#include <unistd.h>
#include <cstring>

using namespace kdi;
using namespace kdi::net;
using namespace warp;
using namespace std;
using namespace ex;

#include <warp/StatTracker.h>
#include <tr1/unordered_map>
#include <boost/thread/mutex.hpp>

namespace {

    class MyTracker
        : public warp::StatTracker,
          public kdi::net::StatReportable
    {
        typedef tr1::unordered_map<std::string, int64_t> map_t;

        map_t stats;
        mutable boost::mutex mutex;

    public:
        void set(strref_t name, int64_t value)
        {
            boost::mutex::scoped_lock lock(mutex);
            stats[name.toString()] = value;
        }

        void add(strref_t name, int64_t delta)
        {
            boost::mutex::scoped_lock lock(mutex);
            stats[name.toString()] += delta;
        }

        void report()
        {
            boost::mutex::scoped_lock lock(mutex);
            for(map_t::const_iterator i = stats.begin();
                i != stats.end(); ++i)
                log("Stat: %s %d", i->first, i->second);
        }

        void getStats(StatReportable::StatMap & out) const
        {
            out.clear();
            boost::mutex::scoped_lock lock(mutex);
            out.insert(stats.begin(), stats.end());
        }
    };

}

#include <kdi/tablet/CachedFragmentLoader.h>
#include <kdi/tablet/CachedLogLoader.h>
#include <kdi/tablet/DiskFragmentLoader.h>
#include <kdi/tablet/DiskFragmentWriter.h>
#include <kdi/tablet/SwitchedFragmentLoader.h>

namespace {

    using namespace kdi::tablet;

    class LoaderAssembly
    {
        boost::scoped_ptr<DiskFragmentLoader>   diskLoader;
        boost::scoped_ptr<CachedFragmentLoader> cachedDiskLoader;
        boost::scoped_ptr<DiskFragmentWriter>   logWriter;
        boost::scoped_ptr<CachedLogLoader>      cachedLogLoader;
        SwitchedFragmentLoader                  switchedLoader;

    public:
        LoaderAssembly(warp::StatTracker * tracker,
                       ConfigManagerPtr const & configMgr)
        {
            diskLoader.reset(new DiskFragmentLoader(tracker));
            cachedDiskLoader.reset(new CachedFragmentLoader(diskLoader.get()));
            logWriter.reset(new DiskFragmentWriter(configMgr));
            cachedLogLoader.reset(
                new CachedLogLoader(cachedDiskLoader.get(), logWriter.get()));

            switchedLoader.setLoader("disk", cachedDiskLoader.get());
            switchedLoader.setLoader("sharedlog", cachedLogLoader.get());
        }

        ~LoaderAssembly() {}

        FragmentLoader * getLoader() { return &switchedLoader; }
    };
}


namespace {

    inline boost::xtime makeTimeout(int64_t sec)
    {
        boost::xtime xt;
        boost::xtime_get(&xt, boost::TIME_UTC);
        xt.sec += sec;
        return xt;
    }


    class SuperTabletServer
    {
        MyTracker * myTracker;

        tablet::MetaConfigManagerPtr metaConfigMgr;
        tablet::FileTrackerPtr tracker;

        boost::scoped_ptr<LoaderAssembly> loader;
        boost::scoped_ptr<DiskFragmentWriter> loggerWriter;
        boost::scoped_ptr<DiskFragmentWriter> compactorWriter;

        tablet::SharedLoggerPtr logger;
        tablet::SharedCompactorPtr compactor;
        tablet::WorkQueuePtr workQueue;
        TablePtr metaTable;
        std::string server;
        ScannerLocator * locator;

        boost::scoped_ptr<tablet::TabletGc> gc;

        boost::scoped_ptr<boost::thread> maintThread;
        boost::mutex maintMutex;
        boost::condition maintCond;
        bool maintExit;

        void maintLoop()
        {
            boost::mutex::scoped_lock lock(maintMutex);

            for(size_t iter = 1; !maintExit; ++iter)
            {
                maintCond.timed_wait(lock, makeTimeout(60));

                if(maintExit)
                    break;

                lock.unlock();

                if(iter % 15 == 0)
                    // Run Tablet GC
                    gc->run();

                if(iter % 15 == 0)
                    // Run Scanner GC
                    locator->purgeAndMark();

                if(iter % 1 == 0)
                    // Run stat report
                    myTracker->report();

                lock.lock();
            }
        }

    public:
        SuperTabletServer(std::string const & root,
                          std::string const & server,
                          ScannerLocator * locator,
                          MyTracker * myTracker) :
            myTracker(myTracker),
            metaConfigMgr(new tablet::MetaConfigManager(root, server)),
            tracker(new tablet::FileTracker),
            loader(new LoaderAssembly(myTracker, metaConfigMgr)),
            loggerWriter(new DiskFragmentWriter(metaConfigMgr)),
            compactorWriter(new DiskFragmentWriter(metaConfigMgr)),
            logger(
                new tablet::SharedLogger(
                    metaConfigMgr,
                    loader->getLoader(),
                    loggerWriter.get(),
                    tracker)),
            compactor(
                new tablet::SharedCompactor(
                    loader->getLoader(),
                    compactorWriter.get(),
                    myTracker)),
            workQueue(new tablet::WorkQueue(1)),
            server(server),
            locator(locator),
            maintExit(false)
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
                loader->getLoader(),
                logger,
                compactor,
                tracker,
                workQueue,
                cfgs.front());

            metaConfigMgr->setMetaTable(metaTable);

            gc.reset(
                new tablet::TabletGc(
                    root, metaTable, Timestamp::now(), server));
            maintThread.reset(
                new boost::thread(
                    callOrDie(
                        boost::bind(
                            &SuperTabletServer::maintLoop,
                            this
                            ),
                        "Maintenance thread", true)));

            kdi::local::IndexCache::setTracker(myTracker);
        }

        ~SuperTabletServer()
        {
            kdi::local::IndexCache::setTracker(0);

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
                            name, metaConfigMgr, loader->getLoader(),
                            logger, compactor, tracker, workQueue
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
                            name, metaConfigMgr, loader->getLoader(),
                            logger, compactor, tracker, workQueue
                            )
                        );
                    return p;
                }
            }
        }

        void shutdown()
        {
            {
                boost::mutex::scoped_lock lock(maintMutex);
                maintExit = true;
                maintCond.notify_all();
            }

            compactor->shutdown();
            workQueue->shutdown();
            logger->shutdown();

            maintThread->join();
        }
    };

    std::string getHostName()
    {
        if(char * env = getenv("KDI_SERVER_HOST"))
        {
            log("Using server name override: %s", env);
            return std::string(env);
        }

        char buf[256];
        if(0 == gethostname(buf, sizeof(buf)) && strcmp(buf, "localhost"))
            return std::string(buf);
        else
            return std::string();
    }

    class ServerApp
        : public virtual Ice::Application
    {
        MyTracker * myTracker;

    public:
        ServerApp(MyTracker * myTracker) : myTracker(myTracker) {}

        void appMain(int ac, char ** av)
        {
            // Set options
            OptionParser op("%prog [ICE-parameters] [options]");
            {
                using namespace boost::program_options;
                op.addOption("root,r", value<string>(),
                             "Root directory for tablet data");
                op.addOption("pidfile,p", value<string>(),
                             "Write PID to file");
                op.addOption("nodaemon", "Don't fork and run as daemon");
            }

            // Parse options
            OptionMap opt;
            ArgumentList args;
            op.parse(ac, av, opt, args);

            // Get table root directory
            string tableRoot;
            if(!opt.get("root", tableRoot))
                op.error("need --root");

            // Write PID file
            string pidFile;
            if(opt.get("pidfile", pidFile))
            {
                FileStream pid(File::output(pidFile));
                pid << getpid() << endl;
                pid.close();
            }

            // Make scanner locator
            ScannerLocator * scannerLocator = new ScannerLocator(50);

            // Create table server
            //   -- hack: tossing the scannerLocator in here so we can
            //   -- have it expire old scanners on the same thread as
            //   -- the Tablet GC.  It doesn't really belong here.
            boost::shared_ptr<SuperTabletServer> server(
                new SuperTabletServer(
                    tableRoot, getHostName(), scannerLocator,
                    myTracker));

            // Create adapter
            Ice::CommunicatorPtr ic = communicator();
            Ice::ObjectAdapterPtr adapter
                = ic->createObjectAdapter("TableAdapter");

            // Create table locator
            TableLocator * tableLocator = new TableLocator(
                boost::bind(
                    &SuperTabletServer::makeTable,
                    server, _1),
                scannerLocator);

            // Install locators
            adapter->addServantLocator(scannerLocator, "scan");
            adapter->addServantLocator(tableLocator, "table");

            // Create TableManager object
            Ice::ObjectPtr object = new ::kdi::net::details::TableManagerI;
            adapter->add(object, ic->stringToIdentity("TableManager"));

            // Create StatReporter object
            adapter->add(
                makeStatReporter(myTracker),
                ic->stringToIdentity("StatReporter"));

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

    void outOfMemory()
    {
        std::cerr << "Operator new failed.  Out of memory." << std::endl;
        _exit(1);
    }

}

int main(int ac, char ** av)
{
    std::set_new_handler(&outOfMemory);

    MyTracker myTracker;

    bool daemonize = true;
    for(int i = 1; i < ac; ++i)
    {
        if(!strcmp(av[i], "--"))
            break;

        if(!strcmp(av[i], "--help") ||
           !strcmp(av[i], "-h") ||
           !strcmp(av[i], "--nodaemon"))
        {
            daemonize = false;
            break;
        }
    }

    if(daemonize)
    {
        if(0 != daemon(1,1))
            raise<RuntimeError>("daemon failed: %s", getStdError());
    }

    ServerApp app(&myTracker);
    return app.main(ac, av);
}
