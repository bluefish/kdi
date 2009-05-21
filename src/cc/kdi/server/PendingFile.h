//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-20
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

#ifndef KDI_SERVER_PENDINGFILE_H
#define KDI_SERVER_PENDINGFILE_H

#include <string>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>

namespace kdi {
namespace server {

    class PendingFile;

    typedef boost::shared_ptr<PendingFile> PendingFilePtr;
    typedef boost::shared_ptr<PendingFile const> PendingFileCPtr;

    class FileTracker;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// PendingFile
//----------------------------------------------------------------------------
class kdi::server::PendingFile
    : private boost::noncopyable
{
public:
    ~PendingFile();

    /// This should be called as soon as possible and only once.
    void assignName(std::string const & filename);

    /// Get the assigned name for this file.  This function should not
    /// be called before assignName().
    std::string const & getName() const;

private:
    explicit PendingFile(FileTracker * tracker);

    friend class FileTracker;

private:
    FileTracker * tracker;
    std::string name;
    bool nameSet;
};

#endif // KDI_SERVER_PENDINGFILE_H
