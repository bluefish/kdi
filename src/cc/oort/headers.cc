//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/oort/headers.cc#1 $
//
// Created 2006/02/17
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "headers.h"

namespace
{
    oort::vega::RecordHeader::Spec const vegaSpec;
    oort::sirius::RecordHeader::Spec const siriusSpec;
}

oort::vega::RecordHeader::Spec const * const oort::VEGA_SPEC = &vegaSpec;
oort::sirius::RecordHeader::Spec const * const oort::SIRIUS_SPEC = &siriusSpec;
