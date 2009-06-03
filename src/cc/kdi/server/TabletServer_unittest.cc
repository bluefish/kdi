//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-10
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
#include <kdi/server/ConfigReader.h>
#include <kdi/server/ConfigWriter.h>
#include <kdi/server/BlockCache.h>
#include <kdi/server/Scanner.h>
#include <kdi/server/name_util.h>

#include <kdi/server/TestConfigReader.h>
#include <kdi/server/DirectBlockCache.h>
#include <kdi/server/NullConfigWriter.h>

#include <kdi/rpc/PackedCellWriter.h>
#include <kdi/rpc/PackedCellReader.h>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <string>
#include <exception>
#include <unittest/main.h>

#include <iostream>
using std::cout;
using std::endl;

using namespace kdi;
using namespace kdi::server;

namespace {

    template <class Base>
    struct TestCb
        : public Base
    {
        bool succeeded;
        bool triggered;
        std::string errorMsg;
        boost::mutex mutex;
        boost::condition cond;

        TestCb() :
            succeeded(false),
            triggered(false)
        {
        }

        void trigger()
        {
            triggered = true;
            cond.notify_all();
        }

        void success()
        {
            boost::mutex::scoped_lock lock(mutex);
            succeeded = true;
            trigger();
        }

        void failure(std::string const & msg)
        {
            boost::mutex::scoped_lock lock(mutex);
            succeeded = false;
            errorMsg = msg;
            trigger();
        }

        void wait()
        {
            boost::mutex::scoped_lock lock(mutex);
            while(!triggered)
                cond.wait(lock);
        }

        void error(std::exception const & ex)
        {
            failure(ex.what());
        }
    };

    struct TestApplyCb
        : public TestCb<TabletServer::ApplyCb>
    {
        int64_t commitTxn;

        void done(int64_t commitTxn)
        {
            this->commitTxn = commitTxn;
            success();
        }
    };

    struct TestSyncCb
        : public TestCb<TabletServer::SyncCb>
    {
        int64_t syncTxn;

        void done(int64_t syncTxn)
        {
            this->syncTxn = syncTxn;
            success();
        }
    };

    struct TestLoadCb
        : public TestCb<TabletServer::LoadCb>
    {
        void done() { success(); }
    };

    struct TestUnloadCb
        : public TestCb<TabletServer::UnloadCb>
    {
        void done() { success(); }
    };

    struct TestScanCb
        : public TestCb<Scanner::ScanCb>
    {
        void done() { success(); }
    };

    std::string getTestCells()
    {
        kdi::rpc::PackedCellWriter writer;
        writer.append("dingos", "ate:", 42, "babies");
        writer.finish();
        return writer.getPacked().toString();
    }

    void checkTestCells(strref_t cells)
    {
        kdi::rpc::PackedCellReader reader(cells);
        
        BOOST_REQUIRE(reader.verifyMagic());
        BOOST_REQUIRE(reader.verifyChecksum());
        BOOST_REQUIRE(reader.verifyOffsets());

        BOOST_REQUIRE_EQUAL(reader.next(), true);
        BOOST_CHECK_EQUAL(reader.getRow(), "dingos");
        BOOST_CHECK_EQUAL(reader.getColumn(), "ate:");
        BOOST_CHECK_EQUAL(reader.getTimestamp(), 42);
        BOOST_CHECK_EQUAL(reader.getValue(), "babies");

        BOOST_CHECK_EQUAL(reader.next(), false);
    }

    void loadTablet(TabletServer * srv, std::string const & tablet)
    {
        TestLoadCb loadCb;
        std::vector<std::string> tablets;
        tablets.push_back(tablet);
        srv->load_async(&loadCb, tablets);

        loadCb.wait();

        BOOST_REQUIRE_EQUAL(loadCb.succeeded, true);
        BOOST_REQUIRE_EQUAL(loadCb.errorMsg, "");
    }
}

BOOST_AUTO_UNIT_TEST(no_table_test)
{
    TabletServer::Bits bits;
    TabletServer server(bits);

    TestApplyCb applyCb;
    server.apply_async(&applyCb, "table", getTestCells(),
                       TabletServer::MAX_TXN, false);

    applyCb.wait();
    
    BOOST_CHECK_EQUAL(applyCb.succeeded, false);
    BOOST_CHECK_EQUAL(applyCb.errorMsg, "TableNotLoadedError");
}

BOOST_AUTO_UNIT_TEST(simple_test)
{
    BOOST_TEST_CHECKPOINT("Init Bits");
    warp::WorkerPool pool(1, "pool", true);
    char const * groups[] = { "ate", 0, 0 };
    TestConfigReader cfgReader(groups);
    NullConfigWriter cfgWriter;

    // Set up the TabletServer bits
    TabletServer::Bits bits;
    bits.workerPool = &pool;
    bits.schemaReader = &cfgReader;
    bits.configReader = &cfgReader;
    bits.configWriter = &cfgWriter;

    // Make a server
    BOOST_TEST_CHECKPOINT("Create ServerInit");
    TabletServer server(bits);

    BOOST_CHECK(server.findTable("table") == 0);

    // Load a single tablet in table "table"
    BOOST_TEST_CHECKPOINT("Load");
    {
        TestLoadCb loadCb;
        std::vector<std::string> tablets;
        tablets.push_back("table!");
        server.load_async(&loadCb, tablets);

        loadCb.wait();

        BOOST_CHECK_EQUAL(loadCb.succeeded, true);
        BOOST_CHECK_EQUAL(loadCb.errorMsg, "");
        BOOST_CHECK(server.findTable("table") != 0);
    }

    // Write a block of test cells
    BOOST_TEST_CHECKPOINT("Apply");
    int64_t commitTxn = -1;
    {
        TestApplyCb applyCb;
        server.apply_async(&applyCb, "table", getTestCells(),
                           TabletServer::MAX_TXN, false);

        applyCb.wait();
    
        BOOST_CHECK_EQUAL(applyCb.succeeded, true);
        BOOST_CHECK_EQUAL(applyCb.errorMsg, "");

        commitTxn = applyCb.commitTxn;
    }

    // Wait until the cells have been logged
    BOOST_TEST_CHECKPOINT("Sync");
    {
        TestSyncCb syncCb;
        server.sync_async(&syncCb, TabletServer::MAX_TXN);

        syncCb.wait();
    
        BOOST_CHECK_EQUAL(syncCb.succeeded, true);
        BOOST_CHECK_EQUAL(syncCb.syncTxn, commitTxn);
        BOOST_CHECK_EQUAL(syncCb.errorMsg, "");
    }

    // Create a scnner to read the cells back
    BOOST_TEST_CHECKPOINT("Create Scanner");
    DirectBlockCache testCache;
    Scanner scanner(
        server.findTable("table"),
        &testCache,
        ScanPredicate(),
        Scanner::SCAN_ANY_TXN);

    BOOST_CHECK(scanner.getLastKey() == 0);
    BOOST_CHECK_EQUAL(scanner.scanContinues(), true);

    // Scan the first block
    BOOST_TEST_CHECKPOINT("Scanning");
    {
        TestScanCb scanCb;
        scanner.scan_async(&scanCb, size_t(-1), size_t(-1));

        scanCb.wait();

        BOOST_CHECK_EQUAL(scanCb.succeeded, true);
        BOOST_CHECK_EQUAL(scanCb.errorMsg, "");

        BOOST_CHECK(scanner.getLastKey() != 0);
        BOOST_CHECK_EQUAL(scanner.getScanTransaction(), commitTxn);
        BOOST_CHECK_EQUAL(scanner.scanContinues(), false);
        BOOST_CHECK(!scanner.getPackedCells().empty());
        
        checkTestCells(scanner.getPackedCells());
    }

#if 0  // not ready yet

    // Unload the table
    BOOST_TEST_CHECKPOINT("Unloading");
    {
        TestUnloadCb unloadCb;
        std::vector<std::string> tablets;
        tablets.push_back("tablet!");
        server.unload_async(&unloadCb, tablets);

        unloadCb.wait();

        BOOST_CHECK_EQUAL(unloadCb.succeeded, true);
        BOOST_CHECK_EQUAL(unloadCb.errorMsg, "");
    }
#endif
}

BOOST_AUTO_UNIT_TEST(wrong_column_test)
{
    char const * groups[] = { "meh", 0, 0 };
    TestConfigReader cfgReader(groups);
    NullConfigWriter cfgWriter;
    TabletServer::Bits bits;
    bits.schemaReader = &cfgReader;
    bits.configReader = &cfgReader;
    bits.configWriter = &cfgWriter;

    TabletServer server(bits);
    loadTablet(&server, "test!");

    kdi::rpc::PackedCellWriter writer;
    writer.append("foo", "bar", 0, "baz");
    writer.finish();

    TestApplyCb applyCb;
    server.apply_async(&applyCb, "test", writer.getPacked(),
                       TabletServer::MAX_TXN, false);

    applyCb.wait();
    
    BOOST_CHECK_EQUAL(applyCb.succeeded, false);
    BOOST_CHECK_EQUAL(applyCb.errorMsg, "UnknownColumnFamilyError");
}
