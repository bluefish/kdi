//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/oort/exception.h#1 $
//
// Created 2006/01/11
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef OORT_EXCEPTION_H
#define OORT_EXCEPTION_H

#include "ex/exception.h"

namespace oort
{
    EX_DECLARE_EXCEPTION(TypeError, ex::Exception);
    EX_DECLARE_EXCEPTION(VersionError, ex::Exception);
}

#endif // OORT_EXCEPTION_H
