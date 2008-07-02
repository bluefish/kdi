//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/LogReader.h $
//
// Created 2008/05/28
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_TABLET_LOGREADER_H
#define KDI_TABLET_LOGREADER_H

#include <kdi/cell.h>
#include <kdi/marshal/cell_block.h>
#include <oort/record.h>
#include <oort/recordstream.h>

namespace kdi {
namespace tablet {

    class LogReader;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// LogReader
//----------------------------------------------------------------------------
class kdi::tablet::LogReader : public kdi::CellStream
{
    oort::RecordStreamHandle input;
    std::string tabletName;

    oort::Record logEntry;
    marshal::CellData const * nextCell;
    marshal::CellData const * endCell;

public:
    LogReader(std::string const & logFn, std::string const & tabletName);
    bool get(Cell & x);
};


#endif // KDI_TABLET_LOGREADER_H
