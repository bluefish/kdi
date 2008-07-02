//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/pack.h#1 $
//
// Created 2007/06/07
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_PACK_H
#define WARP_PACK_H

#include <stdint.h>

/// Pack four bytes into an MSB ordered integer
#define WARP_PACK4(b0, b1, b2, b3) ( ((uint32_t(b3) & 0xff) << 24) |    \
                                     ((uint32_t(b2) & 0xff) << 16) |    \
                                     ((uint32_t(b1) & 0xff) <<  8) |    \
                                     ((uint32_t(b0) & 0xff)      ) )

#endif // WARP_PACK_H
