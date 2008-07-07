//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-03-21
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

#ifndef WARP_FILE_ITERATOR_H
#define WARP_FILE_ITERATOR_H

#include <warp/bufferedfile.h>
#include <boost/iterator.hpp>

namespace warp {

    class FileIterator;

} // namespace warp

//----------------------------------------------------------------------------
// FileIterator
//----------------------------------------------------------------------------
class warp::FileIterator
    : public boost::iterator_facade<FileIterator, char,
            std::random_access_iterator_tag,
            char, off_t>
{
    BufferedFilePtr fp;
    off_t pos;

    friend class boost::iterator_core_access;

    char const dereference() const
    {
        return fp->get(pos);
    }

    bool equal(FileIterator const & o) const
    {
        return fp == o.fp && pos == o.pos;
    }

    void increment() { ++pos; }
    void decrement() { --pos; }
    void advance(off_t o) { pos += o; }

    off_t distance_to(FileIterator const & o) const
    {
        return o.pos - pos;
    }

public:
    FileIterator() : pos(0) {}

    explicit FileIterator(BufferedFilePtr const & fp) :
        fp(fp), pos(0)
    {
        EX_CHECK_NULL(fp);
        pos = fp->tell();
    }

    FileIterator(BufferedFilePtr const & fp, off_t pos) :
        fp(fp), pos(pos)
    {
    }

    explicit FileIterator(FilePtr const & fp_) :
        fp(new BufferedFile(fp_)), pos(fp->tell())
    {
    }

    FileIterator(FilePtr const & fp, off_t pos) :
        fp(new BufferedFile(fp)), pos(pos)
    {
    }

    FileIterator make_end() const
    {
        fp->seek(0, SEEK_END);
        return FileIterator(fp);
    }
};


#endif // WARP_FILE_ITERATOR_H
