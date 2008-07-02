//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/ex/trace_test.cc#1 $
//
// Created 2007/07/25
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "ex/exception.h"
using namespace ex;

void goBoom()
{
    raise<RuntimeError>("I has an error, kthxbye.");
}

int main(int ac, char ** av)
{
    goBoom();
    return 0;
}
