//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-01-11
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

#include <pykdi/pytable.h>
#include <pykdi/pyscan.h>
#include <kdi/scan_predicate.h>
#include <boost/python.hpp>

using namespace pykdi;
using namespace kdi;
using namespace std;
using namespace boost::python;

//----------------------------------------------------------------------------
// PyTable
//----------------------------------------------------------------------------
PyTable::PyTable(string const & uri) :
    uri(uri),
    table(Table::open(uri))
{
}

void PyTable::set(string const & row, string const & col,
                  int64_t rev, string const & val)
{
    table->set(row, col, rev, val);
}

void PyTable::erase(string const & row, string const & col,
                    int64_t rev)
{
    table->erase(row, col, rev);
}

void PyTable::sync()
{
    table->sync();
}

PyScan PyTable::scan() const
{
    return PyScan(table->scan());
}

PyScan PyTable::scan(ScanPredicate const & pred) const
{
    return PyScan(table->scan(pred));
}

PyScan PyTable::scan(std::string const & pred) const
{
    return PyScan(table->scan(pred));
}

void PyTable::defineWrapper()
{
    PyScan (PyTable::*scan1)() const                      = &PyTable::scan;
    PyScan (PyTable::*scan2)(ScanPredicate const &) const = &PyTable::scan;
    PyScan (PyTable::*scan3)(std::string const &) const   = &PyTable::scan;

    class_<PyTable>("Table",
                    "Python interface for a KDI Table.",
                    init<string>())
        .def("set", &PyTable::set,
             "Set a (row,column,version,value) cell in the Table.")
        .def("erase", &PyTable::erase,
             "Erase a cell with given (row,column,version) prefix.")
        .def("sync", &PyTable::sync,
             "Block until mutations have been committed.")
        .def("scan", scan1,
             "Scan all cells in the Table.")
        .def("scan", scan2,
             "Scan cells in the Table using a ScanPredicate.")
        .def("scan", scan3,
             "Scan cells in the Table using a predicate expression.")
        ;
}
