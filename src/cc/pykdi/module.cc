//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-01-11
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#include <pykdi/pytable.h>
#include <pykdi/pyscan.h>
#include <pykdi/conversion.h>
#include <boost/python.hpp>

using namespace pykdi;
using namespace boost::python;

#include <kdi/scan_predicate.h>
#include <boost/lexical_cast.hpp>

using boost::lexical_cast;

namespace
{
    BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(lb_overloads, setLowerBound, 1, 2);
    BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(ub_overloads, setUpperBound, 1, 2);

    template <class T>
    void defineIntervalWrapper(std::string const & prefix)
    {
        typedef warp::Interval<T> IVal;
        typedef warp::IntervalSet<T> ISet;

        IVal & (IVal::*setLowerBound)(T const &, warp::BoundType) = &IVal::setLowerBound;
        IVal & (IVal::*setUpperBound)(T const &, warp::BoundType) = &IVal::setUpperBound;

        class_<IVal>((prefix+"Interval").c_str())
            .def("setLowerBound", setLowerBound,
                 lb_overloads(
                     "Set a the lower bound value and type.  The default\n"
                     "type is INCLUSIVE for an inclusive lower bound.  If\n"
                     "the type is INFINITE, the value doesn't matter."
                     )[return_self<>()]
                )
            .def("setUpperBound", setUpperBound,
                 ub_overloads(
                     "Set the upper bound value and type.  The default\n"
                     "type is EXCLUSIVE for an exclusive upper bound.  If\n"
                     "the type is INFINITE, the value doesn't matter."
                     )[return_self<>()]
                )
            .def("unsetLowerBound", &IVal::unsetLowerBound, return_self<>(),
                 "Unset the lower bound.  This is equivalent to setting an\n"
                 "infinite lower bound."
                )
            .def("unsetUpperBound", &IVal::unsetUpperBound, return_self<>(),
                 "Unset the upper bound.  This is equivalent to setting an\n"
                 "infinite upper bound."
                )
            .def("setPoint", &IVal::setPoint, return_self<>(),
                 "Set the interval to contain a single point.  This is\n"
                 "equivalent to calling setLowerBound and setUpperBound\n"
                 "with the same value and an inclusive type."
                )
            .def("setInfinite", &IVal::setInfinite, return_self<>(),
                 "Set the interval to contain everything.  This makes both\n"
                 "the upper and lower bound infinite."
                )
            .def("setEmpty", &IVal::setEmpty, return_self<>(),
                 "Set the interval to contain nothing.  This makes both\n"
                 "the upper and lower bound exclusive around the same point."
                )
            .def("__str__", &lexical_cast<std::string,IVal>)
            ;

        
        bool (ISet::*contains1)(T const &) const = &ISet::contains;
        bool (ISet::*contains2)(IVal const &) const = &ISet::contains;

        class_<ISet>((prefix+"IntervalSet").c_str())
            //.def("__iter__", iterator<ISet const>())
            .def("__len__", &ISet::size)
            .def("__contains__", contains1)
            .def("__contains__", contains2)
            .def("add", &ISet::add, return_self<>(),
                 "Update the interval set by setting it to the union of\n"
                 "itself and the given interval."
                )
            .def("overlaps", &ISet::overlaps,
                 "True iff the set contains any of the given interval."
                )
            .def("contains", contains1,
                 "True iff the set contains the given point."
                )
            .def("contains", contains2,
                 "True iff the set contains the entire given interval."
                )
            .def("__str__", &lexical_cast<std::string,ISet>)
            ;
    }
}


BOOST_PYTHON_MODULE(pykdi)
{
    //docstring_options doc_opts(true);

    to_python_converter<warp::str_data_t, strdata_converter>();

    PyTable::defineWrapper();
    PyScan::defineWrapper();

    enum_<warp::BoundType>("Bound")
        .value("INFINITE",  warp::BT_INFINITE)
        .value("INCLUSIVE", warp::BT_INCLUSIVE)
        .value("EXCLUSIVE", warp::BT_EXCLUSIVE)
        ;

    defineIntervalWrapper<std::string>("String");
    defineIntervalWrapper<int64_t>("Integer");

    typedef kdi::ScanPredicate Pred;
    class_<Pred>(
        "ScanPredicate",
        "A ScanPredicate can be constructed from an expression string.\n"
        "The expression grammar is:\n"
        "\n"
        "  PREDICATE    := ( SUBPREDICATE ( 'and' SUBPREDICATE )* )?\n"
        "  SUBPREDICATE := INTERVALSET | HISTORY\n"
        "  INTERVALSET  := INTERVAL ( 'or' INTERVAL )*\n"
        "  INTERVAL     := ( LITERAL REL-OP )? FIELD REL-OP LITERAL\n"
        "  HISTORY      := 'history' '=' INTEGER\n"
        "  LITERAL      := STRING-LITERAL | TIME-LITERAL\n"
        "  TIME-LITERAL := ISO8601-DATETIME | ( '@' INTEGER )\n"
        "  FIELD        := 'row' | 'column' | 'time'\n"
        "  REL-OP       := '<' | '<=' | '>' | '>=' | '=' | '~='\n"
        "\n"
        "The STRING-LITERALs may be double- or single-quoted strings.\n"
        "The following backslash escape sequences are supported:\n"
        "  - C-style: \\n,\\r,\\t,\\f,\\a,\\v,\\\\,\\\",\\'\n"
        "  - Hex: \\xXX, where each 'X' is a hex character\n"
        "  - Unicode: \\uXXXX, where each 'X' is a hex character\n"
        "  - Unicode: \\UXXXXXXXX, where each 'X' is a hex character\n"
        "C and Hex escapes will be replaced with their raw byte value.\n"
        "Unicode escapes will be replaced with the UTF-8 encoding of\n"
        "the character.\n"
        "\n"
        "The familiar REL-OPs all have the usual interpretation.  The\n"
        "odd one, '~=', is a 'starts-with' operator.  For example:\n"
        "    column ~= \"group:\"\n"
        "means that columns starting with \"group:\" should be selected.\n"
        "\n"
        "The grammar is approximate.  Here are some restrictions:\n"
        "  - different FIELDs cannot be mixed in the same INTERVALSET\n"
        "  - there can be only one INTERVALSET per FIELD\n"
        "  - there can be only one HISTORY\n"
        "  - STRING-LITERALS can only be used with 'row' and 'column'\n"
        "  - TIME-LITERALS can only be used with 'time'\n"
        "  - the '~=' operator only works with STRING-LITERALs\n"
        "  - the extended INTERVAL form only works when the two REL-OPs\n"
        "    are inequalities in the same direction (e.g. both '<' or\n"
        "    '<=', or both '>' or '>=')\n"
        "\n"
        "ISO8601-DATETIMEs will be converted to an integer representing\n"
        "the number of microseconds from 1970-01-01T00:00:00.000000Z to\n"
        "the given timestamp.  If the timezone is omitted from the\n"
        "given time, it assumed to be in local time.\n"
        "\n"
        "Predicate examples:\n"
        "   row = 'com.foo.www/index.html' and history = 1\n"
        "   row ~= 'com.foo' and time >= 1999-01-02T03:04:05.678901Z\n"
        "   \"word:cat\" < column <= \"word:dog\" or column >= \"word:fish\"\n"
        "   time = @0"
        )
        .def(init<std::string>())
        .def("isConstrained", &Pred::isConstrained,
             "Return true if this predicate is more restrictive than a\n"
             "full cell scan.  The default predicate is unconstrained.\n"
             "Filtering constraints are specified by calling the 'set'\n"
             "methods."
            )
        .def("setRowPredicate", &Pred::setRowPredicate, return_self<>(),
             "Restrict the scan to the given set of rows.  Only cells with\n"
             "row names contained in the given interval set will be returned\n"
             "by the scan.  The default behavior is to return all cells."
            )
        .def("setColumnPredicate", &Pred::setColumnPredicate, return_self<>(),
             "Restrict the scan to the given set of columns.  Only cells\n"
             "with column names contained in the given interval set will be\n"
             "returned by the scan.  The default behavior is to return all\n"
             "cells."
            )
        .def("setTimePredicate", &Pred::setTimePredicate, return_self<>(),
             "Restrict the scan to the given set of timestamps.  Only cells\n"
             "with timestamps contained in the given interval set will be\n"
             "returned by the scan.  The default behavior is to return all\n"
             "cells."
            )
        .def("setMaxHistory", &Pred::setMaxHistory, return_self<>(),
             "Set the maximum history length in the scan.  Cells in the scan\n"
             "have unique (row, column, timestamp) keys, but there may be\n"
             "many cells with the same row and column but different\n"
             "timestamps.  These can be interpreted as historical context\n"
             "for a cell value.  The maximum history parameter restricts the\n"
             "scan to the latest K timestamps for each unique (row,column)\n"
             "pair.  A negative value for K means that everything\n"
             "*except* the latest K timestamps are returned.  Setting K\n"
             "to zero means return all timestamps.  This is the default\n"
             "behavior.  Note that this predicate is applied as filter\n"
             "*after* the timestamp interval set predicate."
            )
        .def("clearRowPredicate", &Pred::clearRowPredicate, return_self<>(),
             "Clear the row constraint.")
        .def("clearColumnPredicate", &Pred::clearColumnPredicate, return_self<>(),
             "Clear the column constraint.")
        .def("clearTimePredicate", &Pred::clearTimePredicate, return_self<>(),
             "Clear the time constraint.")
        .def("__str__", &lexical_cast<std::string,Pred>)
        ;
}
