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

#ifndef PYKDI_CONVERSION_H
#define PYKDI_CONVERSION_H

#include <warp/strutil.h>
#include <boost/python.hpp>

namespace pykdi {

    /// Convert warp::str_data_t to Python string objects.
    struct strdata_converter
    {
        static PyObject * convert(warp::str_data_t const & s)
        {
            return PyString_FromStringAndSize(s.begin(), s.size());
        }
    };

} // namespace pykdi

#endif // PYKDI_CONVERSION_H
