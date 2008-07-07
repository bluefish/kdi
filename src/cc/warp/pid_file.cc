//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-08-23
// 
// This file is part of the warp library.
// 
// The warp library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The warp library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#include <warp/pid_file.h>
#include <warp/file.h>
#include <warp/fs.h>
#include <ex/exception.h>
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
