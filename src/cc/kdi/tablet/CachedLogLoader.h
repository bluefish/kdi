//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-11-04
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

#ifndef KDI_TABLET_CACHEDLOGLOADER_H
#define KDI_TABLET_CACHEDLOGLOADER_H

#include <kdi/tablet/FragmentLoader.h>
#include <kdi/tablet/FragmentWriter.h>
#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/shared_ptr.hpp>
#include <string>
#include <map>

namespace kdi {
namespace tablet {

    class CachedLogLoader;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// CachedLogLoader
//----------------------------------------------------------------------------
class kdi::tablet::CachedLogLoader
    : public kdi::tablet::FragmentLoader,
      private boost::noncopyable
{
    struct LogInfo;

    FragmentLoader * loader;
    FragmentWriter * writer;

    mutable std::map<std::string, boost::shared_ptr<LogInfo> > logMap;
    mutable boost::mutex mutex;
    mutable boost::mutex writerMutex;

public:
    CachedLogLoader(FragmentLoader * loader, FragmentWriter * writer);

    FragmentPtr load(std::string const & uri) const;

private:
    /// Get the disk URI for the given log URI.  If the log hasn't
    /// been loaded yet, it will be serialized to a disk file.  Future
    /// requests for the same log will use the result of the first
    /// serialization.
    std::string const & getDiskUri(std::string const & logUri) const;
};

#endif // KDI_TABLET_CACHEDLOGLOADER_H
