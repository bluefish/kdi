//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/LogWriter.cc $
//
// Created 2008/05/20
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/tablet/LogWriter.h>
#include <kdi/tablet/LogEntry.h>
#include <kdi/marshal/cell_block_builder.h>
#include <oort/recordbuilder.h>
#include <oort/fileio.h>
#include <warp/uri.h>

using namespace kdi;
using namespace kdi::tablet;
using namespace kdi::marshal;
using namespace oort;
using namespace warp;
using namespace std;

//----------------------------------------------------------------------------
// LogWriter
//----------------------------------------------------------------------------
LogWriter::LogWriter(string const & fileUri) :
    fp(File::output(fileUri)),
    uri(uriPushScheme(fileUri, "sharedlog"))
{
    logStream = makeBuildStream<LogEntry>(128<<10);
    logStream->pipeTo(FileOutput::make(fp));
}

void LogWriter::sync()
{
    // XXX Should have a way to sync the file, not just flush.
    logStream->flush();

    //fp->flush(); // not necessary after log flush
}

void LogWriter::logCells(string const & tabletName,
                         vector<Cell> const & cells)
{
    /// XXX the output format should have a checksum so we can recover
    /// from mid-write crashes.

    BOOST_STATIC_ASSERT(LogEntry::VERSION == 1);

    // Make a RecordBuilder and build the LogEntry.  A LogEntry is a
    // fixed-size type derived from a CellBlock.  The CellBlockBuilder
    // will write the base CellBlock, then we'll append the derived
    // tabletName.
    RecordBuilder b;
    CellBlockBuilder cb(&b);
    b << (*b.subblock(4) << StringData::wrapStr(tabletName));

    // Fill the CellBlock
    for(vector<Cell>::const_iterator i = cells.begin(); i != cells.end(); ++i)
        cb.append(*i);

    // Write the built entry
    logStream->put(b);
}
