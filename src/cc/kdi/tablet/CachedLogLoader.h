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

#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/shared_ptr.hpp>
#include <string>
#include <map>

namespace kdi {
namespace tablet {

    class CachedLogLoader;

    // Forward declaration
    class ConfigManager;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// CachedLogLoader
//----------------------------------------------------------------------------
class kdi::tablet::CachedLogLoader
    : private boost::noncopyable
{
    struct LogInfo;
        
    std::map<std::string, boost::shared_ptr<LogInfo> > logMap;
    boost::mutex mutex;

public:
    /// Get the disk URI for the given log URI.  If the log hasn't
    /// been loaded yet, it will be serialized to a disk file.  Future
    /// requests for the same log will use the result of the first
    /// serialization.
    std::string const & getDiskUri(std::string const & logUri,
                                   ConfigManager * configManager);
};

#endif // KDI_TABLET_CACHEDLOGLOADER_H
