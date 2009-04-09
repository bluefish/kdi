//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-07
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

#ifndef KDI_SERVER_TESTCONFIGREADER_H
#define KDI_SERVER_TESTCONFIGREADER_H

#include <kdi/server/ConfigReader.h>

namespace kdi {
namespace server {

    class TestConfigReader;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// TestConfigReader
//----------------------------------------------------------------------------
struct kdi::server::TestConfigReader
    : public ConfigReader
{
public:
    void readSchemas_async(
        ReadSchemasCb * cb,
        std::vector<std::string> const & tableNames);

    void readConfigs_async(
        ReadConfigsCb * cb,
        std::vector<std::string> const & tabletNames);
};

#endif // KDI_SERVER_TESTCONFIGREADER_H
