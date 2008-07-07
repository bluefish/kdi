//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-14
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

#include <unittest/main.h>
#include <kdi/marshal/cell_block_builder.h>
#include <vector>

using namespace warp;
using namespace kdi;
using namespace kdi::marshal;
using std::vector;

BOOST_AUTO_UNIT_TEST(basic)
{
    Builder builder;
    CellBlockBuilder cellBuilder(&builder);

    // Check that builder contains no cells
    BOOST_CHECK_EQUAL(cellBuilder.getCellCount(), 0ul);
    BOOST_CHECK(cellBuilder.getDataSize() > 0);
    size_t emptyDataSize = cellBuilder.getDataSize();

    // Add some cells
    cellBuilder.appendCell("test", "one", 100, "two");
    cellBuilder.append(makeCell("test", "two", 100, "three"));
    cellBuilder.appendErasure("test", "one", 100);
    cellBuilder.append(makeCellErasure("test", "two", 100));
    
    // Check that builder contains 4 cells
    BOOST_CHECK_EQUAL(cellBuilder.getCellCount(), 4ul);
    BOOST_CHECK(cellBuilder.getDataSize() > emptyDataSize);

    // Build CellBlock
    builder.finalize();
    vector<char> buffer(builder.getFinalSize());
    BOOST_REQUIRE(!buffer.empty());
    builder.exportTo(&buffer[0]);

    // Reset builder
    builder.reset();
    cellBuilder.reset();

    // Check that builder contains no cells
    BOOST_CHECK_EQUAL(cellBuilder.getCellCount(), 0ul);
    BOOST_CHECK_EQUAL(cellBuilder.getDataSize(), emptyDataSize);
    
    // Make sure we built the right thing
    CellBlock const * block = reinterpret_cast<CellBlock const *>(&buffer[0]);
    BOOST_REQUIRE_EQUAL(block->cells.size(), 4u);

    CellData const * c = block->cells.begin();
    BOOST_CHECK_EQUAL(c->key.row->toString(), "test");
    BOOST_CHECK_EQUAL(c->key.column->toString(), "one");
    BOOST_CHECK_EQUAL(c->key.timestamp, 100u);
    BOOST_CHECK_EQUAL(c->value->toString(), "two");

    ++c;
    BOOST_CHECK_EQUAL(c->key.row->toString(), "test");
    BOOST_CHECK_EQUAL(c->key.column->toString(), "two");
    BOOST_CHECK_EQUAL(c->key.timestamp, 100u);
    BOOST_CHECK_EQUAL(c->value->toString(), "three");

    ++c;
    BOOST_CHECK_EQUAL(c->key.row->toString(), "test");
    BOOST_CHECK_EQUAL(c->key.column->toString(), "one");
    BOOST_CHECK_EQUAL(c->key.timestamp, 100u);
    BOOST_CHECK(c->value.isNull());

    ++c;
    BOOST_CHECK_EQUAL(c->key.row->toString(), "test");
    BOOST_CHECK_EQUAL(c->key.column->toString(), "two");
    BOOST_CHECK_EQUAL(c->key.timestamp, 100u);
    BOOST_CHECK(c->value.isNull());

    ++c;
    BOOST_CHECK_EQUAL(c, block->cells.end());
}
