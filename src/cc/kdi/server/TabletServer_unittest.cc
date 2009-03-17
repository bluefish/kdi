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
#include <kdi/rpc/PackedCellWriter.h>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <string>
#include <exception>
#include <unittest/main.h>

using namespace kdi;
using namespace kdi::server;

namespace {

    struct TestCb
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
    };

    struct TestApplyCb
        : public TabletServer::ApplyCb,
          public TestCb
    {
        int64_t commitTxn;

        void done(int64_t commitTxn)
        {
            this->commitTxn = commitTxn;
            success();
        }

        void error(std::exception const & ex)
        {
            failure(ex.what());
        }
    };

    struct TestSyncCb
        : public TabletServer::SyncCb,
          public TestCb
    {
        int64_t syncTxn;

        void done(int64_t syncTxn)
        {
            this->syncTxn = syncTxn;
            success();
        }

        void error(std::exception const & ex)
        {
            failure(ex.what());
        }
    };

    struct NullLogWriter
        : public LogWriter
    {
    public:
        void writeCells(strref_t tableName, CellBufferCPtr const & cells) {}
        size_t getDiskSize() const { return 0; }
        void sync() {}
        void close() {}

        static LogWriter * make()
        {
            return new NullLogWriter;
        }
    };

    std::string getTestCells()
    {
        kdi::rpc::PackedCellWriter writer;
        writer.append("dingos", "ate", 42, "babies");
        writer.finish();
        return writer.getPacked().toString();
    }
}

BOOST_AUTO_UNIT_TEST(notable_test)
{
    TabletServer server(&NullLogWriter::make, 0, 0);

    TestApplyCb applyCb;
    server.apply_async(&applyCb, "table", getTestCells(),
                       TabletServer::MAX_TXN, false);

    applyCb.wait();
    
    BOOST_CHECK_EQUAL(applyCb.succeeded, false);
    BOOST_CHECK_EQUAL(applyCb.errorMsg, "TableNotLoadedError");
}

BOOST_AUTO_UNIT_TEST(simple_test)
{
    warp::WorkerPool pool(1, "pool", true);
    TabletServer server(&NullLogWriter::make, &pool, 0);

    TestApplyCb applyCb;
    server.apply_async(&applyCb, "table", getTestCells(),
                       TabletServer::MAX_TXN, false);

    applyCb.wait();
    
    BOOST_CHECK_EQUAL(applyCb.succeeded, true);
    BOOST_CHECK_EQUAL(applyCb.errorMsg, "");

    TestSyncCb syncCb;
    server.sync_async(&syncCb, TabletServer::MAX_TXN);

    syncCb.wait();
    
    BOOST_CHECK_EQUAL(syncCb.succeeded, true);
    BOOST_CHECK_EQUAL(syncCb.syncTxn, applyCb.commitTxn);
    BOOST_CHECK_EQUAL(syncCb.errorMsg, "");
}
