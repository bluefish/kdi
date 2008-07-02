//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/file_iterator.h $
//
// Created 2008/03/21
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
