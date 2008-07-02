//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/pykdi/pyscan.h#1 $
//
// Created 2008/01/11
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
