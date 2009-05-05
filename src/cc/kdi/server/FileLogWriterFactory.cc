//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-04
//
// This file is part of KDI.
//
// KDI is free software; you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the Free Software
// Foundation; either version 2 of the License, or any later version.
//
// KDI is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
// details.
//
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#include <kdi/server/FileLogWriterFactory.h>
#include <kdi/server/FileLogWriter.h>
#include <warp/fs.h>
#include <warp/file.h>
#include <ex/exception.h>
#include <sstream>

using namespace kdi::server;
using namespace warp;
using namespace ex;

//----------------------------------------------------------------------------
// FileLogWriterFactory
//----------------------------------------------------------------------------
FileLogWriterFactory::FileLogWriterFactory(std::string const & logRoot) :
    logRoot(logRoot),
    logIdx(0)
{
    fs::makedirs(logRoot);
    if(!fs::isEmpty(logRoot))
        raise<RuntimeError>("log directory is not empty: %s", logRoot);
}

std::auto_ptr<LogWriter> FileLogWriterFactory::start()
{
    std::ostringstream oss;
    oss << logIdx;
    ++logIdx;

    std::string fn = fs::resolve(logRoot, oss.str());
    FilePtr fp = File::open(fn, O_WRONLY | O_CREAT | O_TRUNC | O_EXCL);

    std::auto_ptr<LogWriter> p(new FileLogWriter(fp));
    return p;
}
