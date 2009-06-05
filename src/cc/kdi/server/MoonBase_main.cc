//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-07
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

#include <kdi/server/TabletServer.h>
#include <kdi/server/TabletServerI.h>
#include <kdi/server/ScannerLocator.h>
#include <warp/options.h>
#include <warp/filestream.h>
#include <warp/fs.h>
#include <warp/log.h>
#include <ex/exception.h>
#include <Ice/Ice.h>
#include <boost/scoped_ptr.hpp>
#include <string>
#include <memory>

#include <warp/WorkerPool.h>
#include <kdi/server/TestConfigReader.h>
#include <kdi/server/DirectBlockCache.h>
#include <kdi/server/FileConfigWriter.h>
#include <kdi/server/DiskWriterFactory.h>
#include <kdi/server/CachedFragmentLoader.h>
#include <kdi/server/DiskLoader.h>
#include <kdi/server/FileLogWriterFactory.h>
#include <kdi/server/FileTracker.h>

using namespace kdi::server;
using namespace kdi;
using namespace warp;
using namespace ex;
using namespace std;

namespace {

    template <class T>
    void release(T * & p)
    {
        delete p;
        p = 0;
    }
        
    class MainServerAssembly
        : private boost::noncopyable
    {
        boost::scoped_ptr<FileTracker> fragFileTracker;
        boost::scoped_ptr<TestConfigReader> configReader;
        boost::scoped_ptr<FileConfigWriter> configWriter;
        boost::scoped_ptr<warp::WorkerPool> workerPool;
        boost::scoped_ptr<TabletServer> server;
        boost::scoped_ptr<DirectBlockCache> cache;
        boost::scoped_ptr<DiskWriterFactory> fragmentFactory;

        boost::scoped_ptr<DiskLoader> diskLoader;
        boost::scoped_ptr<CachedFragmentLoader> cachedLoader;

        boost::scoped_ptr<FileLogWriterFactory> logFactory;

        struct LoadCb : public TabletServer::LoadCb
        {
            boost::mutex mutex;
            boost::condition cond;
            std::string errorMsg;
            bool complete;
            bool success;
            
            LoadCb() : complete(false), success(false) {}
            
            void done() throw()
            {
                boost::mutex::scoped_lock lock(mutex);
                complete = true;
                success = true;
                cond.notify_all();
            }

            void error(std::exception const & ex) throw()
            {
                boost::mutex::scoped_lock lock(mutex);
                complete = true;
                success = false;
                errorMsg = ex.what();
                cond.notify_all();
            }

            void wait()
            {
                boost::mutex::scoped_lock lock(mutex);
                while(!complete)
                    cond.wait(lock);
                if(!success)
                    throw std::runtime_error(errorMsg);
            }
        };

    public:
        ~MainServerAssembly() { cleanup(); }

        TabletServer * getServer() const { return server.get(); }
        BlockCache * getBlockCache() const { return cache.get(); }

        void init(string const & dataRoot,
                  string const & logDir,
                  string const & configDir,
                  string const & location)
        {
            fragFileTracker.reset(new FileTracker(dataRoot));

            workerPool.reset(new WorkerPool(4, "Pool", true));
            char const *groups[] = { "group", 0, 0 };
            configReader.reset(new TestConfigReader(groups));
            configWriter.reset(new FileConfigWriter(configDir));
            fragmentFactory.reset(
                new DiskWriterFactory(dataRoot, fragFileTracker.get()));

            diskLoader.reset(new DiskLoader(dataRoot));
            cachedLoader.reset(new CachedFragmentLoader(diskLoader.get()));

            logFactory.reset(new FileLogWriterFactory(logDir));

            TabletServer::Bits bits;
            bits.schemaReader = configReader.get();
            bits.configReader = configReader.get();
            bits.configWriter = configWriter.get();
            bits.workerPool = workerPool.get();
            bits.fragmentFactory = fragmentFactory.get();
            bits.fragmentLoader = cachedLoader.get();
            bits.logFactory = logFactory.get();
            bits.serverLogDir = logDir;
            bits.serverLocation = location;
            
            server.reset(new TabletServer(bits));
            cache.reset(new DirectBlockCache);

            log("Loading: foo!");
            LoadCb cb;
            std::vector<std::string> tablets;
            tablets.push_back("foo!");
            server->load_async(&cb, tablets);
            cb.wait();
            log("Loaded.");
        }

        void cleanup()
        {
            cache.reset();
            server.reset();
            cachedLoader.reset();
            diskLoader.reset();
            configReader.reset();
            configWriter.reset();
            fragmentFactory.reset();
            workerPool.reset();
            fragFileTracker.reset();
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
                op.addOption("dataRoot,d", value<string>(),
                             "Root directory for tablet data");
                op.addOption("logDir,l", value<string>(),
                             "Private directory for logging tablet data");
                op.addOption("configDir,c", value<string>(),
                             "Directory for tablet configs");
                op.addOption("pidfile,p", value<string>(),
                             "Write PID to file");
                op.addOption("nodaemon", "Don't fork and run as daemon");
            }

            // Parse options
            OptionMap opt;
            ArgumentList args;
            op.parse(ac, av, opt, args);

            // Get table root directory
            string dataRoot;
            if(!opt.get("dataRoot", dataRoot))
                op.error("need --dataRoot");

            // Get log directory
            string logDir;
            if(!opt.get("logDir", logDir) || !fs::isRooted(logDir))
                op.error("need absolute --logDir");
            if(fs::exists(logDir) && (!fs::isDirectory(logDir) || !fs::isEmpty(logDir)))
                op.error("--logDir already exists and is not empty");

            // Get config directory
            string configDir;
            if(!opt.get("configDir", configDir) || !fs::isRooted(configDir))
                op.error("need absolute --configDir");

            // Write PID file
            string pidfile;
            if(opt.get("pidfile", pidfile))
            {
                FileStream pid(File::output(pidfile));
                pid << getpid() << endl;
                pid.close();
            }

            // Make scanner locator
            ScannerLocator * scannerLocator = new ScannerLocator(1000);

            // Create adapter
            Ice::CommunicatorPtr ic = communicator();
            Ice::ObjectAdapterPtr readWriteAdapter
                = ic->createObjectAdapter("ReadWriteAdapter");

            // Install locators
            readWriteAdapter->addServantLocator(scannerLocator, "scan");

            // Get server location
            Ice::Identity serverId = ic->stringToIdentity("TabletServer");
            std::string location = readWriteAdapter->createProxy(serverId)->ice_toString();

            // Make our TabletServer
            MainServerAssembly serverAssembly;
            serverAssembly.init(dataRoot, logDir, configDir, location);

            // Create TableManagerI object
            Ice::ObjectPtr obj = new TabletServerI(serverAssembly.getServer(),
                                                   scannerLocator,
                                                   serverAssembly.getBlockCache());
            readWriteAdapter->add(obj, serverId);

            // Run server
            readWriteAdapter->activate();
            ic->waitForShutdown();

            // Shutdown
            log("Shutting down");
            scannerLocator->purgeAndMark();
            scannerLocator->purgeAndMark();
            serverAssembly.cleanup();

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

    ServerApp app;
    return app.main(ac, av);
}
