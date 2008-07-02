//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: pid_file.cc $
//
// Created 2007/08/23
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "pid_file.h"
#include "file.h"
#include "fs.h"
#include "ex/exception.h"
#include <unistd.h>
#include <stdio.h>

using namespace warp;
using namespace ex;

PidFile::PidFile(std::string const & fn) :
    fn(fn)
{
    char buf[64];
    int sz = snprintf(buf, sizeof(buf), "%d\n", getpid());
    if(sz < 0)
        raise<RuntimeError>("couldn't write PID into a buffer: %s",
                            getStdError());

    FilePtr fp = File::output(fn);
    fp->write(buf, sz);
    fp->close();
}

PidFile::~PidFile()
{
    fs::remove(fn);
}
