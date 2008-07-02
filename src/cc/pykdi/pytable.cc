//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/pykdi/pytable.cc#1 $
//
// Created 2008/01/11
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
