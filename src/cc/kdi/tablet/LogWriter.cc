//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-20
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

#include <kdi/tablet/LogWriter.h>
#include <kdi/tablet/LogEntry.h>
#include <kdi/marshal/cell_block_builder.h>
#include <oort/recordbuilder.h>
#include <oort/fileio.h>
#include <oort/buildstream.h>
#include <warp/file.h>
#include <warp/uri.h>

using namespace kdi;
using namespace kdi::tablet;
using namespace kdi::marshal;
using namespace oort;
using namespace warp;
using namespace std;

//----------------------------------------------------------------------------
// LogWriter::Impl
//----------------------------------------------------------------------------
class LogWriter::Impl
{
    FilePtr fp;
    string uri;

    BuildStreamHandle logStream;
    RecordBuilder recBuilder;
    CellBlockBuilder cellBuilder;

public:
    Impl(string const & fileUri) :
        fp(File::output(fileUri)),
        uri(uriPushScheme(fileUri, "sharedlog")),
        cellBuilder(&recBuilder)
    {
        logStream = makeBuildStream<LogEntry>(128<<10);
        logStream->pipeTo(FileOutput::make(fp));
    }

    void sync()
    {
        // XXX Should have a way to sync the file, not just flush.
        logStream->flush();

        //fp->flush(); // not necessary after log flush
    }

    void logCells(string const & tabletName,
                  vector<Cell> const & cells)
    {
        /// XXX the output format should have a checksum so we can recover
        /// from mid-write crashes.

        BOOST_STATIC_ASSERT(LogEntry::VERSION == 1);

        // Make a RecordBuilder and build the LogEntry.  A LogEntry is a
        // fixed-size type derived from a CellBlock.  The CellBlockBuilder
        // will write the base CellBlock, then we'll append the derived
        // tabletName.
        recBuilder << (*recBuilder.subblock(4) << StringData::wrap(tabletName));

        // Fill the CellBlock
        for(vector<Cell>::const_iterator i = cells.begin(); i != cells.end(); ++i)
            cellBuilder.append(*i);

        // Write the built entry
        logStream->put(recBuilder);

        // Reset the CellBlockBuilder from the RecordBuilder.  The
        // RecordBuilder has already been reset.
        cellBuilder.reset(&recBuilder);
    }

    string const & getUri() const
    {
        return uri;
    }
};


//----------------------------------------------------------------------------
// LogWriter
//----------------------------------------------------------------------------
LogWriter::LogWriter(string const & fileUri) :
    impl(new Impl(fileUri))
{
}

void LogWriter::sync()
{
    impl->sync();
}

void LogWriter::logCells(string const & tabletName,
                         vector<Cell> const & cells)
{
    impl->logCells(tabletName, cells);
}

string const & LogWriter::getUri() const
{
    return impl->getUri();
}
