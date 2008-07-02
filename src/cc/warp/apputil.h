//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/apputil.h#1 $
//
// Created 2006/01/10
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
