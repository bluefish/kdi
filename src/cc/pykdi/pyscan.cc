//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/pykdi/pyscan.cc#1 $
//
// Created 2008/01/11
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "pyscan.h"

using namespace pykdi;
using namespace kdi;
using namespace boost::python;
using boost::python::objects::stop_iteration_error;

//----------------------------------------------------------------------------
// PyScan
//----------------------------------------------------------------------------
PyScan::PyScan(CellStreamPtr const & scan) :
    scan(scan)
{
}

object PyScan::next()
{
    Cell x;
    if(!scan->get(x))
        stop_iteration_error();

    return make_tuple(
        str(x.getRow()),
        str(x.getColumn()),
        x.getTimestamp(),
        str(x.getValue()));
}

void PyScan::defineWrapper()
{
    class_<PyScan>("Scan",
                   "Iterator over a sequence of Table cells.",
                   no_init)
        .def("next", &PyScan::next,
             "Get the next (row,column,version,value) cell in the scan.")
        .def("__iter__", &PyScan::iter, return_self<>(),
             "Return self (an iterable object).")
        ;
}
