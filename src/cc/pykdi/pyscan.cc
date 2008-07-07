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

#include <pykdi/pyscan.h>

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
