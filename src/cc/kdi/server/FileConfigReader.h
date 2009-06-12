//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-06-11
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

#ifndef KDI_SERVER_FILECONFIGREADER_H
#define KDI_SERVER_FILECONFIGREADER_H

#include <kdi/server/ConfigReader.h>
#include <vector>

namespace kdi {
namespace server {

    class FileConfigReader;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// FileConfigReader
//----------------------------------------------------------------------------
class kdi::server::FileConfigReader
    : public kdi::server::ConfigReader
{
public:
    explicit FileConfigReader(std::string const & configDir);

    virtual TabletConfigCPtr readConfig(std::string const & tabletName);

    std::vector<TabletConfigCPtr> readAllConfigs();
    std::vector<std::string> readAllNames();

private:
    std::string const configDir;
};


#endif // KDI_SERVER_FILECONFIGREADER_H
