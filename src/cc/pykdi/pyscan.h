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

#ifndef PYKDI_PYSCAN_H
#define PYKDI_PYSCAN_H

#include <kdi/cell.h>
#include <boost/python.hpp>

namespace pykdi {

    class PyScan;

} // namespace pykdi


//----------------------------------------------------------------------------
// PyScan
//----------------------------------------------------------------------------
class pykdi::PyScan
{
    kdi::CellStreamPtr scan;

public:
    explicit PyScan(kdi::CellStreamPtr const & scan);
    boost::python::object next();
    void iter() {}

public:
    static void defineWrapper();
};

#endif // PYKDI_PYSCAN_H
