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

#ifndef OORT_FILEIO_H
#define OORT_FILEIO_H

#include <oort/record.h>
#include <flux/stream.h>
#include <vector>

namespace oort
{
    class FileInput;
    class FileOutput;

    class FileInputPositioner;
}


//----------------------------------------------------------------------------
// Forward declaration
//----------------------------------------------------------------------------
namespace warp
{
    class File;
}


//----------------------------------------------------------------------------
// FileInput
//----------------------------------------------------------------------------
class oort::FileInput : public flux::Stream<Record>
{
public:
    typedef FileInput my_t;
    typedef boost::shared_ptr<my_t> handle_t;

    typedef boost::shared_ptr<Allocator> alloc_t;
    typedef boost::shared_ptr<warp::File> file_t;

private:
    file_t file;
    HeaderSpec const * spec;
    alloc_t alloc;
    char * hdrBuf;
    std::vector<char> lzoBuffer;

public:
    /// Make FileInput with VEGA_SPEC and a 1mb buffer allocator
    explicit FileInput(file_t const & file);

    FileInput(file_t const & file, HeaderSpec const * spec,
              alloc_t const & alloc);
    ~FileInput();

    FileInput(FileInput const & o);
    FileInput const & operator=(FileInput const & o);

    void setFile(file_t const & file);
    void setHeaderSpec(HeaderSpec const * spec);
    void setAllocator(alloc_t const & alloc);

    void seek(off_t pos);

    bool get(Record & r);

    std::string getName() const;

    // Open Vega input streams
    static handle_t make(file_t const & fp);
    static handle_t make(file_t const & fp, alloc_t const & alloc);
    static handle_t make(file_t const & fp, size_t allocSz);
};


//----------------------------------------------------------------------------
// FileOutput
//----------------------------------------------------------------------------
class oort::FileOutput : public flux::Stream<Record>
{
public:
    typedef FileOutput my_t;
    typedef boost::shared_ptr<my_t> handle_t;

    typedef boost::shared_ptr<warp::File> file_t;

private:
    file_t file;
    HeaderSpec const * spec;
    char * hdrBuf;
    std::vector<char> lzoBuffer;

public:
    explicit FileOutput(file_t const & file = file_t(),
                        HeaderSpec const * spec = 0);
    ~FileOutput();

    FileOutput(FileOutput const & o);
    FileOutput const & operator=(FileOutput const & o);

    void setFile(file_t const & file);
    void setHeaderSpec(HeaderSpec const * spec);

    void flush();
    void put(Record const & r);

    std::string getName() const;

    // Open Vega output stream
    static handle_t make(file_t const & fp);
};


#endif // OORT_FILEIO_H
