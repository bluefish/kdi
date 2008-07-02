//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: pid_file.h $
//
// Created 2007/08/23
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_PID_FILE_H
#define WARP_PID_FILE_H

#include <string>
#include <boost/noncopyable.hpp>

namespace warp
{
    /// Write a file containing the process ID (PID) to a specified
    /// location.  The file is removed when the object is destroyed.
    class PidFile;
}

//----------------------------------------------------------------------------
// PidFile
//----------------------------------------------------------------------------
class warp::PidFile : private boost::noncopyable
{
    std::string fn;

public:
    explicit PidFile(std::string const & fn);
    ~PidFile();

    std::string const & getName() const { return fn; }
};

#endif // WARP_PID_FILE_H
