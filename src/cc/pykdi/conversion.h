//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/pykdi/conversion.h#1 $
//
// Created 2008/01/11
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
