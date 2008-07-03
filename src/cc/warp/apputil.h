//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-10
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef WARP_APPUTIL_H
#define WARP_APPUTIL_H

#include "util.h"
#include "generator.h"
#include "file.h"

#include <string>
#include <vector>
#include <iostream>
#include <algorithm>
#include <cstdlib>

namespace warp
{
    //------------------------------------------------------------------------
    // findFilesWithExtension
    //------------------------------------------------------------------------
    void findFilesWithExtension(std::string const & root,
                                std::string const & ext,
                                std::vector<std::string> & files);

    //------------------------------------------------------------------------
    // findInputFiles
    //------------------------------------------------------------------------
    void findInputFiles(std::vector<std::string> const & inputArgs,
                        std::string const & fileExt,
                        std::vector<std::string> & result);

    //------------------------------------------------------------------------
    // cleanup_vector
    //------------------------------------------------------------------------
    template <class T, class DestructOp=delete_ptr>
    class cleanup_vector : public std::vector<T>
    {
        typedef std::vector<T> super;
        DestructOp destructOp;

    public:
        ~cleanup_vector()
        {
            std::for_each(this->begin(), this->end(), destructOp);
        }

        void clear()
        {
            std::for_each(this->begin(), this->end(), destructOp);
            super::clear();
        }
    };

    //------------------------------------------------------------------------
    // InputFileGenerator
    //------------------------------------------------------------------------
    class InputFileGenerator : public Generator<FilePtr>
    {
    public:
        typedef std::vector<std::string> string_vec_t;
    
    private:
        string_vec_t filenames;
        size_t idx;
        bool verbose;
    
    public:
        InputFileGenerator(string_vec_t const & filenames, bool verbose=false);

        virtual FilePtr next();
    };

    //------------------------------------------------------------------------
    // SeqFnGenerator
    //------------------------------------------------------------------------
    class SeqFnGenerator : public Generator<std::string>
    {
        std::string base;
        std::string fnFmt;
        int idx;

    public:
        SeqFnGenerator(std::string const & base, std::string const & fnFmt,
                       int startIdx=0);

        virtual std::string next();
    };
}


#endif // WARP_APPUTIL_H
