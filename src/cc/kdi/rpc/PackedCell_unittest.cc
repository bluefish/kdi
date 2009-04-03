//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-09
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

#include <kdi/rpc/PackedCellReader.h>
#include <kdi/rpc/PackedCellWriter.h>
#include <unittest/main.h>
#include <warp/repr.h>
#include <iostream>

using namespace kdi;
using namespace kdi::rpc;

BOOST_AUTO_UNIT_TEST(packed_test_basic)
{
    PackedCellWriter writer;

    BOOST_CHECK_EQUAL(writer.getCellCount(), 0u);

    writer.append("foo", "bar", 7, "llama");

    BOOST_CHECK_EQUAL(writer.getCellCount(), 1u);
    BOOST_CHECK_EQUAL(writer.getLastRow(), "foo");
    BOOST_CHECK_EQUAL(writer.getLastColumn(), "bar");
    BOOST_CHECK_EQUAL(writer.getLastTimestamp(), 7);

    writer.append("foo", "dingo", 3, "");

    BOOST_CHECK_EQUAL(writer.getCellCount(), 2u);
    BOOST_CHECK_EQUAL(writer.getLastRow(), "foo");
    BOOST_CHECK_EQUAL(writer.getLastColumn(), "dingo");
    BOOST_CHECK_EQUAL(writer.getLastTimestamp(), 3);

    writer.appendErasure("foo", "bar", 6);

    BOOST_CHECK_EQUAL(writer.getCellCount(), 3u);
    BOOST_CHECK_EQUAL(writer.getLastRow(), "foo");
    BOOST_CHECK_EQUAL(writer.getLastColumn(), "bar");
    BOOST_CHECK_EQUAL(writer.getLastTimestamp(), 6);

    writer.finish();

    BOOST_CHECK_EQUAL(writer.getCellCount(), 3u);
    BOOST_CHECK_EQUAL(writer.getLastRow(), "foo");
    BOOST_CHECK_EQUAL(writer.getLastColumn(), "bar");
    BOOST_CHECK_EQUAL(writer.getLastTimestamp(), 6);

    std::cerr << warp::ReprEscape(writer.getPacked()) << std::endl;

    PackedCellReader reader;
    reader.reset(writer.getPacked());
    
    BOOST_CHECK_EQUAL(reader.verifyMagic(), true);
    BOOST_CHECK_EQUAL(reader.verifyChecksum(), true);
    BOOST_CHECK_EQUAL(reader.verifyOffsets(), true);
    
    BOOST_CHECK_EQUAL(reader.next(), true);
    BOOST_CHECK_EQUAL(reader.getRow(), "foo");
    BOOST_CHECK_EQUAL(reader.getColumn(), "bar");
    BOOST_CHECK_EQUAL(reader.getTimestamp(), 7);
    BOOST_CHECK_EQUAL(reader.getValue(), "llama");
    BOOST_CHECK_EQUAL(reader.isErasure(), false);

    BOOST_CHECK_EQUAL(reader.next(), true);
    BOOST_CHECK_EQUAL(reader.getRow(), "foo");
    BOOST_CHECK_EQUAL(reader.getColumn(), "dingo");
    BOOST_CHECK_EQUAL(reader.getTimestamp(), 3);
    BOOST_CHECK_EQUAL(reader.getValue(), "");
    BOOST_CHECK_EQUAL(reader.isErasure(), false);

    BOOST_CHECK_EQUAL(reader.next(), true);
    BOOST_CHECK_EQUAL(reader.getRow(), "foo");
    BOOST_CHECK_EQUAL(reader.getColumn(), "bar");
    BOOST_CHECK_EQUAL(reader.getTimestamp(), 6);
    BOOST_CHECK_EQUAL(reader.getValue(), "");
    BOOST_CHECK_EQUAL(reader.isErasure(), true);

    BOOST_CHECK_EQUAL(reader.next(), false);
    BOOST_CHECK_EQUAL(reader.next(), false);
}
