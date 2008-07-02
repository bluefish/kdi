//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/LogEntry.h $
//
// Created 2008/05/20
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_TABLET_LOGENTRY_H
#define KDI_TABLET_LOGENTRY_H

#include <kdi/marshal/cell_block.h>

namespace kdi {
namespace tablet {

    struct LogEntry;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// LogEntry
//----------------------------------------------------------------------------
struct kdi::tablet::LogEntry
    : public kdi::marshal::CellBlock
{
    enum {
        TYPECODE = WARP_PACK4('T','b','L','g'),
        VERSION = 1 + kdi::marshal::CellBlock::VERSION,
        FLAGS = 0,
        ALIGNMENT = 8,
    };

    warp::StringOffset tabletName;
};

#endif // KDI_TABLET_LOGENTRY_H
