//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-11-16
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

#include <kdi/CellField.h>
#include <kdi/row_buffer.h>
#include <unittest/main.h>
#include <algorithm>

using namespace kdi;

BOOST_AUTO_UNIT_TEST(field_comparison_test)
{
    Cell x = makeCell("row-x", "fam-x:qual-x", 0, "val-x");

    // Field == Cell
    BOOST_CHECK(CellRow            ("row-x")         == x);
    BOOST_CHECK(CellColumn         ("fam-x:qual-x")  == x);
    BOOST_CHECK(CellColumnFamily   ("fam-x")         == x);
    BOOST_CHECK(CellColumnQualifier("qual-x")        == x);
    BOOST_CHECK(CellTimestamp      (0)               == x);
    BOOST_CHECK(CellValue          ("val-x")         == x);

    // !(Field == Cell)
    BOOST_CHECK(!(CellRow            ("row-y")         == x));
    BOOST_CHECK(!(CellColumn         ("fam-x:qual-y")  == x));
    BOOST_CHECK(!(CellColumnFamily   ("fam-y")         == x));
    BOOST_CHECK(!(CellColumnQualifier("qual-y")        == x));
    BOOST_CHECK(!(CellTimestamp      (1)               == x));
    BOOST_CHECK(!(CellValue          ("val-y")         == x));

    // Field < Cell
    BOOST_CHECK(CellRow            ("row-w")         < x);
    BOOST_CHECK(CellColumn         ("fam-x:qual-w")  < x);
    BOOST_CHECK(CellColumnFamily   ("fam-w")         < x);
    BOOST_CHECK(CellColumnQualifier("qual-w")        < x);
    BOOST_CHECK(CellTimestamp      (-1)              < x);
    BOOST_CHECK(CellValue          ("val-w")         < x);

    // !(Field < Cell) [equal]
    BOOST_CHECK(!(CellRow            ("row-x")         < x));
    BOOST_CHECK(!(CellColumn         ("fam-x:qual-x")  < x));
    BOOST_CHECK(!(CellColumnFamily   ("fam-x")         < x));
    BOOST_CHECK(!(CellColumnQualifier("qual-x")        < x));
    BOOST_CHECK(!(CellTimestamp      (0)               < x));
    BOOST_CHECK(!(CellValue          ("val-x")         < x));

    // !(Field < Cell) [greater]
    BOOST_CHECK(!(CellRow            ("row-y")         < x));
    BOOST_CHECK(!(CellColumn         ("fam-x:qual-y")  < x));
    BOOST_CHECK(!(CellColumnFamily   ("fam-y")         < x));
    BOOST_CHECK(!(CellColumnQualifier("qual-y")        < x));
    BOOST_CHECK(!(CellTimestamp      (1)               < x));
    BOOST_CHECK(!(CellValue          ("val-y")         < x));


    // Cell == Field
    BOOST_CHECK(x == CellRow            ("row-x")       );
    BOOST_CHECK(x == CellColumn         ("fam-x:qual-x"));
    BOOST_CHECK(x == CellColumnFamily   ("fam-x")       );
    BOOST_CHECK(x == CellColumnQualifier("qual-x")      );
    BOOST_CHECK(x == CellTimestamp      (0)             );
    BOOST_CHECK(x == CellValue          ("val-x")       );

    // !(Cell == Field)
    BOOST_CHECK(!(x == CellRow            ("row-y")       ));
    BOOST_CHECK(!(x == CellColumn         ("fam-x:qual-y")));
    BOOST_CHECK(!(x == CellColumnFamily   ("fam-y")       ));
    BOOST_CHECK(!(x == CellColumnQualifier("qual-y")      ));
    BOOST_CHECK(!(x == CellTimestamp      (1)             ));
    BOOST_CHECK(!(x == CellValue          ("val-y")       ));

    // Cell < Field
    BOOST_CHECK(x < CellRow            ("row-y")       );
    BOOST_CHECK(x < CellColumn         ("fam-x:qual-y"));
    BOOST_CHECK(x < CellColumnFamily   ("fam-y")       );
    BOOST_CHECK(x < CellColumnQualifier("qual-y")      );
    BOOST_CHECK(x < CellTimestamp      (1)             );
    BOOST_CHECK(x < CellValue          ("val-y")       );

    // !(Cell < Field) [equal]
    BOOST_CHECK(!(x < CellRow            ("row-x")       ));
    BOOST_CHECK(!(x < CellColumn         ("fam-x:qual-x")));
    BOOST_CHECK(!(x < CellColumnFamily   ("fam-x")       ));
    BOOST_CHECK(!(x < CellColumnQualifier("qual-x")      ));
    BOOST_CHECK(!(x < CellTimestamp      (0)             ));
    BOOST_CHECK(!(x < CellValue          ("val-x")       ));

    // !(Cell < Field) [greater]
    BOOST_CHECK(!(x < CellRow            ("row-w")       ));
    BOOST_CHECK(!(x < CellColumn         ("fam-x:qual-w")));
    BOOST_CHECK(!(x < CellColumnFamily   ("fam-w")       ));
    BOOST_CHECK(!(x < CellColumnQualifier("qual-w")      ));
    BOOST_CHECK(!(x < CellTimestamp      (-1)            ));
    BOOST_CHECK(!(x < CellValue          ("val-w")       ));
}

BOOST_AUTO_UNIT_TEST(prefix_limits_test)
{
    Cell x = makeCell("row-x", "fam-x:qual-x", 0, "val-x");

    // Check limits: Prefix == Cell
    BOOST_CHECK(CellRowPrefix("")         == x);
    BOOST_CHECK(CellRowPrefix("r")        == x);
    BOOST_CHECK(CellRowPrefix("ro")       == x);
    BOOST_CHECK(CellRowPrefix("row")      == x);
    BOOST_CHECK(CellRowPrefix("row-")     == x);
    BOOST_CHECK(CellRowPrefix("row-x")    == x);
    BOOST_CHECK(!(CellRowPrefix("row-xx") == x));
    BOOST_CHECK(!(CellRowPrefix("row-y")  == x));
    BOOST_CHECK(!(CellRowPrefix("row=x")  == x));
    BOOST_CHECK(!(CellRowPrefix("row=")   == x));

    // Check limits: Cell == Prefix
    BOOST_CHECK(x == CellRowPrefix("")        );
    BOOST_CHECK(x == CellRowPrefix("r")       );
    BOOST_CHECK(x == CellRowPrefix("ro")      );
    BOOST_CHECK(x == CellRowPrefix("row")     );
    BOOST_CHECK(x == CellRowPrefix("row-")    );
    BOOST_CHECK(x == CellRowPrefix("row-x")   );
    BOOST_CHECK(!(x == CellRowPrefix("row-xx")));
    BOOST_CHECK(!(x == CellRowPrefix("row-y") ));
    BOOST_CHECK(!(x == CellRowPrefix("row=x") ));
    BOOST_CHECK(!(x == CellRowPrefix("row=")  ));
    
    // Check limits: Prefix < Cell
    BOOST_CHECK(!(CellRowPrefix("")         < x));
    BOOST_CHECK(!(CellRowPrefix("r")        < x));
    BOOST_CHECK(!(CellRowPrefix("ro")       < x));
    BOOST_CHECK(!(CellRowPrefix("row")      < x));
    BOOST_CHECK(!(CellRowPrefix("row-")     < x));
    BOOST_CHECK(!(CellRowPrefix("row-x")    < x));
    BOOST_CHECK(!(CellRowPrefix("row-xx")   < x));
    BOOST_CHECK(!(CellRowPrefix("row-y")    < x));
    BOOST_CHECK(!(CellRowPrefix("roy")      < x));
    BOOST_CHECK(!(CellRowPrefix("rp")       < x));
    BOOST_CHECK(!(CellRowPrefix("s")        < x));
    BOOST_CHECK(CellRowPrefix("row-w")      < x);
    BOOST_CHECK(CellRowPrefix("rov")        < x);
    BOOST_CHECK(CellRowPrefix("rn")         < x);
    BOOST_CHECK(CellRowPrefix("q")          < x);

    // Check limits: Cell < Prefix
    BOOST_CHECK(!(x < CellRowPrefix("")      ));
    BOOST_CHECK(!(x < CellRowPrefix("r")     ));
    BOOST_CHECK(!(x < CellRowPrefix("ro")    ));
    BOOST_CHECK(!(x < CellRowPrefix("row")   ));
    BOOST_CHECK(!(x < CellRowPrefix("row-")  ));
    BOOST_CHECK(!(x < CellRowPrefix("row-x") ));
    BOOST_CHECK(x < CellRowPrefix("row-xx")   );
    BOOST_CHECK(x < CellRowPrefix("row-y")    );
    BOOST_CHECK(x < CellRowPrefix("roy")      );
    BOOST_CHECK(x < CellRowPrefix("rp")       );
    BOOST_CHECK(x < CellRowPrefix("s")        );
    BOOST_CHECK(!(x < CellRowPrefix("row-w") ));
    BOOST_CHECK(!(x < CellRowPrefix("rov")   ));
    BOOST_CHECK(!(x < CellRowPrefix("rn")    ));
    BOOST_CHECK(!(x < CellRowPrefix("q")     ));
}

BOOST_AUTO_UNIT_TEST(prefix_fields_test)
{
    Cell x = makeCell("row-x", "fam-x:qual-x", 0, "val-x");

    BOOST_CHECK(CellRowPrefix("row")              == x);
    BOOST_CHECK(CellColumnPrefix("fam-x:qu")      == x);
    BOOST_CHECK(CellColumnFamilyPrefix("fam")     == x);
    BOOST_CHECK(CellColumnQualifierPrefix("qual") == x);
    BOOST_CHECK(CellValuePrefix("val")            == x);
}

BOOST_AUTO_UNIT_TEST(rowbuffer_algo)
{
    RowBuffer row;

    row.append(makeCell("foo", "fam1:qual1", 1, "timmy-1"));
    row.append(makeCell("foo", "fam1:qual1", 0, "timmy-0"));
    row.append(makeCell("foo", "fam1:qual2", 2, "dingo"));
    row.append(makeCell("foo", "fam1:qual3", 3, "frob"));
    row.append(makeCell("foo", "fam3:qual1", 3, "timmy"));
    row.append(makeCell("foo", "fam3:qual4", 0, "dingo"));
    row.append(makeCell("foo", "fam3:qual5", 1, "wonk"));
    row.append(makeCell("foo", "fam3:qual6", 2, "waka"));

    BOOST_CHECK_EQUAL(row.size(), 8u);

    typedef RowBuffer::cell_iterator iter;
    typedef std::pair<iter, iter> iterpair;

    BOOST_CHECK_EQUAL(std::find(row.begin(), row.end(), CellValue("dingo"))->getColumn(), "fam1:qual2");
    BOOST_CHECK(std::lower_bound(row.begin(), row.end(), CellColumnPrefix("fam3:")) == row.begin()+4);
    iterpair p = std::equal_range(row.begin(), row.end(), CellColumnFamily("fam1"));
    BOOST_CHECK_EQUAL(p.second - p.first, 4u);
    BOOST_CHECK_EQUAL(p.first->getColumn(), "fam1:qual1");
    BOOST_CHECK_EQUAL(p.second->getColumn(), "fam3:qual1");
}

