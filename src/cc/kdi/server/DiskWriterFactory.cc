//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-29
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

#include <kdi/server/DiskWriterFactory.h>
#include <kdi/server/TableSchema.h>
#include <kdi/server/DiskWriter.h>
#include <warp/fs.h>
#include <warp/file.h>
#include <warp/tuple.h>
#include <cassert>

using namespace kdi::server;
using namespace warp;
using namespace std;

//----------------------------------------------------------------------------
// DiskWriterFactory
//----------------------------------------------------------------------------
std::auto_ptr<FragmentWriter>
DiskWriterFactory::start(TableSchema const & schema, int groupIndex)
{
    std::auto_ptr<FragmentWriter> p;

    assert(groupIndex >= 0 && (size_t)groupIndex < schema.groups.size());
    assert(!schema.tableName.empty());

    string tableDir = fs::resolve(dataRoot, schema.tableName);

    fs::makedirs(tableDir);

    string path = fs::resolve(tableDir, "$(UNIQUE)");
    FilePtr fp;
    tie(fp, path) = File::openUnique(path);

    string fn = schema.tableName + "/" + fs::basename(path);
    p.reset(new DiskWriter(fp, fn, schema.groups[groupIndex].diskBlockSize));
    return p;
}
