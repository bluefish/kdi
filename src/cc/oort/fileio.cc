//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-11
//
// This file is part of the oort library.
//
// The oort library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
//
// The oort library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#include <oort/fileio.h>
#include <oort/recordbuffer.h>
#include <oort/headers.h>
#include <warp/file.h>
#include <ex/exception.h>
#include <assert.h>
#include <boost/format.hpp>
#include <minilzo/minilzo.h>

using namespace oort;
using namespace warp;
using namespace ex;

//----------------------------------------------------------------------------
// allocHeaderBuf
//----------------------------------------------------------------------------
namespace
{
    inline void allocHeaderBuf(char * & buf, HeaderSpec const * spec)
    {
        delete[] buf;
        if(spec)
            buf = new char[spec->headerSize()];
        else
            buf = 0;
    }

    inline void initLzo()
    {
        static bool needInit = true;
        if(needInit)
        {
            int err = lzo_init();
            if(err != LZO_E_OK)
                raise<RuntimeError>("failed to init LZO: err=%d", err);

            needInit = false;
        }
    }

}


//----------------------------------------------------------------------------
// FileInput
//----------------------------------------------------------------------------
FileInput::FileInput(file_t const & file) :
    file(file), spec(VEGA_SPEC),
    alloc(new RecordBufferAllocator(1<<20)),
    hdrBuf(0)
{
    allocHeaderBuf(hdrBuf, spec);
}

FileInput::FileInput(file_t const & file, HeaderSpec const * spec,
                     alloc_t const & alloc) :
    file(file), spec(spec), alloc(alloc), hdrBuf(0)
{
    allocHeaderBuf(hdrBuf, spec);
}

FileInput::~FileInput()
{
    delete[] hdrBuf;
}

FileInput::FileInput(FileInput const & o) :
    file(o.file), spec(o.spec), alloc(o.alloc), hdrBuf(0)
{
    allocHeaderBuf(hdrBuf, spec);
}

FileInput const & FileInput::operator=(FileInput const & o)
{
    file = o.file;
    spec = o.spec;
    alloc = o.alloc;
    allocHeaderBuf(hdrBuf, spec);
    return *this;
}

void FileInput::setFile(file_t const & f)
{
    file = f;
}

void FileInput::setHeaderSpec(HeaderSpec const * s)
{
    spec = s;
    allocHeaderBuf(hdrBuf, spec);
}

void FileInput::setAllocator(alloc_t const & a)
{
    alloc = a;
}

void FileInput::seek(off_t pos)
{
    assert(file);
    file->seek(pos);
}

bool FileInput::get(Record & r)
{
    assert(file);
    assert(spec);
    assert(alloc);
    assert(hdrBuf);

    off_t pos = file->tell();
    try {
        if(!file->read(hdrBuf, spec->headerSize(), 1))
            return false;

        HeaderSpec::Fields f;
        spec->deserialize(hdrBuf, f);

        char * dataPtr;
        bool isCompressed = f.flags & HeaderSpec::LZO_COMPRESSED;
        if(isCompressed) {
            // Record is compressed, read into lzoBuffer
            lzoBuffer.clear();
            lzoBuffer.resize(f.length);
            dataPtr = &lzoBuffer[0];
        }
        else {
            // Not compressed, read directly into Record
            dataPtr = alloc->alloc(r, f);
        }

        size_t dataSz = file->read(dataPtr, f.length);
        if(dataSz < f.length)
            raise<IOError>("short record: got %1% bytes of %2% (%3%:%4%)",
                           dataSz, f.length, file->getName(), pos);

        if(isCompressed) {
            lzo_uint compressedSize = f.length - sizeof(uint32_t);
            lzo_uint uncompressedSize = deserialize<uint32_t>(dataPtr);

            f.length = uncompressedSize;
            char * recordPtr = alloc->alloc(r, f);

            // Make sure LZO is ready
            initLzo();

            // Decompress buffer into record
            lzo1x_decompress_safe(
                reinterpret_cast<const lzo_bytep>(dataPtr + sizeof(uint32_t)),
                compressedSize,
                reinterpret_cast<lzo_bytep>(recordPtr),
                &uncompressedSize,
                NULL);
        }

        return true;
    }
    catch(Exception const & ex) {
        raise<IOError>("failed to get record (%1%:%2%): %3%",
                       file->getName(), pos, ex.what());
    }
}

std::string FileInput::getName() const
{
    using boost::format;

    if(file)
        return (format("%s#%s") % file->getName() % file->tell()).str();
    else
        return std::string("null");
}

FileInput::handle_t FileInput::make(file_t const & fp)
{
    alloc_t alloc(new RecordBufferAllocator());
    handle_t h(new FileInput(fp, VEGA_SPEC, alloc));
    return h;
}

FileInput::handle_t FileInput::make(file_t const & fp, alloc_t const & alloc)
{
    handle_t h(new FileInput(fp, VEGA_SPEC, alloc));
    return h;
}

FileInput::handle_t FileInput::make(file_t const & fp, size_t allocSz)
{
    alloc_t alloc(new RecordBufferAllocator(allocSz));
    handle_t h(new FileInput(fp, VEGA_SPEC, alloc));
    return h;
}


//----------------------------------------------------------------------------
// FileOutput
//----------------------------------------------------------------------------
FileOutput::FileOutput(file_t const & file, HeaderSpec const * spec) :
    file(file), spec(spec), hdrBuf(0)
{
    allocHeaderBuf(hdrBuf, spec);
}

FileOutput::~FileOutput()
{
    delete[] hdrBuf;
}

FileOutput::FileOutput(FileOutput const & o) :
    file(file), spec(spec), hdrBuf(0)
{
    allocHeaderBuf(hdrBuf, spec);
}

FileOutput const & FileOutput::operator=(FileOutput const & o)
{
    file = o.file;
    spec = o.spec;
    allocHeaderBuf(hdrBuf, spec);
    return *this;
}

void FileOutput::setFile(file_t const & f)
{
    file = f;
}

void FileOutput::setHeaderSpec(HeaderSpec const * s)
{
    spec = s;
    allocHeaderBuf(hdrBuf, spec);
}

void FileOutput::flush()
{
    assert(file);
    file->flush();
}

void FileOutput::put(Record const & r)
{
    assert(file);
    assert(spec);
    assert(hdrBuf);

    HeaderSpec::Fields f(r);
    char const * outputData = r.getData();

    // Did the record header request compression?
    if(r.getFlags() & HeaderSpec::LZO_COMPRESSED) {
        // Make sure LZO is ready to roll
        initLzo();

        // Working memory for LZO compression
        unsigned char lzoWorkingMemory[LZO1X_MEM_COMPRESS];

        // In the worst case, LZO may bloat the input data instead of compressing
        // Docs say an extra 16 bytes per 1024 bytes of input should be safe
        size_t bufferSize = f.length + 16*(1+f.length/1024);

        // Reserve space for compression buffer + originalSize field
        lzoBuffer.clear();
        lzoBuffer.resize(bufferSize + sizeof(uint32_t));
        lzo_bytep compressedData = reinterpret_cast<lzo_bytep>(
            &lzoBuffer[sizeof(uint32_t)]);

        // Compress into lzoBuffer (after originalSize field)
        lzo_uint compressedSize;
        lzo1x_1_compress(
            reinterpret_cast<const lzo_bytep>(outputData),
            static_cast<lzo_uint>(f.length),
            compressedData,
            &compressedSize,
            lzoWorkingMemory);

        // See if resulting compression ratio is worth keeping.  We'll
        // only use compression if we get better than 1/16th
        // compression savings (that is, compressed size is less than
        // 15/16 the size of the original data).  There's nothing
        // particularly magical about 1/16th except that it is fast to
        // compute.
        if(compressedSize + sizeof(uint32_t) < ((size_t(f.length)*15)/16)) {
            // Got some savings, use the compressed data
            outputData = &lzoBuffer[0];

            // Write the originalSize in the payload
            serialize(&lzoBuffer[0], uint32_t(f.length));

            // Rewrite the header
            f.length = compressedSize + sizeof(uint32_t);
        } else {
            // Compression isn't worth it, clear compression flag
            f.flags ^= HeaderSpec::LZO_COMPRESSED;
        }
    }

    spec->serialize(f, hdrBuf);
    if(!file->write(hdrBuf, spec->headerSize(), 1))
        raise<IOError>("couldn't write header for Record %s to '%s'",
                       r.toString(), file->getName());

    size_t dataSz = file->write(outputData, f.length);
    if(dataSz < f.length)
        raise<IOError>("couldn't write data for Record %s (wrote %d bytes) "
                       "to '%s'", r.toString(), dataSz, file->getName());
}

std::string FileOutput::getName() const
{
    using boost::format;

    if(file)
        return (format("%s#%s") % file->getName() % file->tell()).str();
    else
        return std::string("null");
}

FileOutput::handle_t FileOutput::make(file_t const & fp)
{
    handle_t h(new FileOutput(fp, VEGA_SPEC));
    return h;
}
