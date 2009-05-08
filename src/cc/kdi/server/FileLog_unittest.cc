//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-05
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

#include <kdi/server/FileLogReader.h>
#include <kdi/server/FileLogWriter.h>
#include <kdi/server/FileLogDirReader.h>
#include <kdi/server/FileLogWriterFactory.h>
#include <kdi/server/FileLog.h>
#include <warp/file.h>
#include <unittest/main.h>

#include <kdi/rpc/PackedCellWriter.h>
#include <kdi/rpc/PackedCellReader.h>

using namespace kdi::server;
using namespace warp;
using kdi::rpc::PackedCellWriter;
using kdi::rpc::PackedCellReader;

BOOST_AUTO_UNIT_TEST(rw_test)
{
    FileLogWriter logWriter(File::output("memfs:log"));
    size_t baseSz = logWriter.getDiskSize();

    logWriter.writeCells("table1", "cells-a");
    logWriter.writeCells("table2", "cells-b");
    logWriter.writeCells("table1", "cells-c");
    logWriter.sync();

    BOOST_CHECK(baseSz < logWriter.getDiskSize());

    logWriter.writeCells("table1", "cells-d");
    logWriter.writeCells("table2", "cells-e");
    logWriter.sync();

    std::string fn = logWriter.finish();
    BOOST_CHECK(!fn.empty());

    FilePtr fp = File::input(fn);
    FileLogHeader hdr;
    BOOST_CHECK_EQUAL(fp->read(&hdr, sizeof(hdr)), sizeof(hdr));
    BOOST_CHECK_EQUAL(hdr.magic, (uint32_t)FileLogHeader::MAGIC);
    BOOST_CHECK_EQUAL(hdr.version, 0u);

    FileLogReaderV0 logReader(fp);
    BOOST_REQUIRE(logReader.next());
    BOOST_CHECK_EQUAL(logReader.getTableName(), "table1");
    BOOST_CHECK_EQUAL(logReader.getPackedCells(), "cells-a");

    BOOST_REQUIRE(logReader.next());
    BOOST_CHECK_EQUAL(logReader.getTableName(), "table2");
    BOOST_CHECK_EQUAL(logReader.getPackedCells(), "cells-b");

    BOOST_REQUIRE(logReader.next());
    BOOST_CHECK_EQUAL(logReader.getTableName(), "table1");
    BOOST_CHECK_EQUAL(logReader.getPackedCells(), "cells-c");

    BOOST_REQUIRE(logReader.next());
    BOOST_CHECK_EQUAL(logReader.getTableName(), "table1");
    BOOST_CHECK_EQUAL(logReader.getPackedCells(), "cells-d");

    BOOST_REQUIRE(logReader.next());
    BOOST_CHECK_EQUAL(logReader.getTableName(), "table2");
    BOOST_CHECK_EQUAL(logReader.getPackedCells(), "cells-e");

    BOOST_CHECK(!logReader.next());
}

BOOST_AUTO_UNIT_TEST(dir_test)
{
    FileLogWriterFactory logFactory("memfs:logdir");

    std::string cellBase = "cell";
    for(char x = 'A'; x != 'G'; ++x)
    {
        std::auto_ptr<LogWriter> w = logFactory.start();
        BOOST_REQUIRE(w.get());

        for(char y = '1'; y != '9'; ++y)
        {
            w->writeCells("table1", cellBase + x + y);
            w->writeCells("table2", cellBase + y + x);
            w->sync();
        }

        w->finish();
    }

    FileLogDirReader dirReader;

    std::auto_ptr<LogDirReader::Iterator> iter =
        dirReader.readLogDir("memfs:logdir");
    BOOST_REQUIRE(iter.get());

    for(char x = 'A'; x != 'G'; ++x)
    {
        std::auto_ptr<LogReader> r = iter->next();
        BOOST_REQUIRE(r.get());

        for(char y = '1'; y != '9'; ++y)
        {
            BOOST_REQUIRE(r->next());
            BOOST_CHECK_EQUAL(r->getTableName(), "table1");
            BOOST_CHECK_EQUAL(r->getPackedCells(), cellBase + x + y);

            BOOST_REQUIRE(r->next());
            BOOST_CHECK_EQUAL(r->getTableName(), "table2");
            BOOST_CHECK_EQUAL(r->getPackedCells(), cellBase + y + x);
        }

        BOOST_CHECK(!r->next());
    }

    BOOST_CHECK(!iter->next().get());
}
