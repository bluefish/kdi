//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-01
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

#include <warp/sysinfo.h>
#include <warp/strutil.h>
#include <fcntl.h>
#include <unistd.h>

using namespace warp;

size_t warp::getPageSize()
{
    return getpagesize();
}

bool warp::readPageCounts(PageCounts * x)
{
    char buf[256];
    int fd = open("/proc/self/statm", O_RDONLY);
    if(fd == -1)
        return false;

    ssize_t rd = read(fd, buf, sizeof(buf));
    close(fd);

    if(rd <= 0)
        return false;

    memset(x, sizeof(PageCounts), 0);

    char const * end = buf + rd;
    char const * p0 = skipSpace((char const *)buf, end);
    char const * p1 = skipNonSpace(p0, end);

    parseInt(x->total, p0, p1);

    p0 = skipSpace(p1, end);
    p1 = skipNonSpace(p0, end);

    parseInt(x->resident, p0, p1);

    p0 = skipSpace(p1, end);
    p1 = skipNonSpace(p0, end);

    parseInt(x->shared, p0, p1);

    p0 = skipSpace(p1, end);
    p1 = skipNonSpace(p0, end);

    parseInt(x->text, p0, p1);

    p0 = skipSpace(p1, end);
    p1 = skipNonSpace(p0, end);

    parseInt(x->lib, p0, p1);

    p0 = skipSpace(p1, end);
    p1 = skipNonSpace(p0, end);

    parseInt(x->data, p0, p1);

    p0 = skipSpace(p1, end);
    p1 = skipNonSpace(p0, end);

    parseInt(x->dirty, p0, p1);

    return true;
}
