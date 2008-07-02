//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/cell.h#1 $
//
// Created 2007/10/23
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_CELL_H
#define KDI_CELL_H

#include <kdi/strref.h>
#include <sdstore/cell.h>
#include <flux/stream.h>
#include <boost/shared_ptr.hpp>

namespace kdi {

    using sdstore::Cell;

    typedef flux::Stream<Cell> CellStream;
    typedef boost::shared_ptr<CellStream> CellStreamPtr;

    using sdstore::makeCell;
    using sdstore::makeCellErasure;

} // namespace kdi

#endif // KDI_CELL_H
