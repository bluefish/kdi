//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-13
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

#ifndef KDI_SERVER_CONFIGREADER_H
#define KDI_SERVER_CONFIGREADER_H

#include <kdi/server/TableSchema.h>
#include <kdi/server/TabletConfig.h>
#include <vector>
#include <string>
#include <exception>

namespace kdi {
namespace server {

    class ConfigReader;

} // namespace server
} // namespace kdi


//----------------------------------------------------------------------------
// ConfigReader
//----------------------------------------------------------------------------
class kdi::server::ConfigReader
{
public:
    class ReadSchemasCb
    {
    public:
        virtual void done(std::vector<TableSchema> const & schemas) = 0;
        virtual void error(std::exception const & err) = 0;
    protected:
        ~ReadSchemasCb() {}
    };

    class ReadConfigsCb
    {
    public:
        virtual void done(std::vector<TabletConfig> const & configs) = 0;
        virtual void error(std::exception const & err) = 0;
    protected:
        ~ReadConfigsCb() {}
    };

public:
    virtual void readSchemas_async(
        ReadSchemasCb * cb,
        std::vector<std::string> const & tableNames) = 0;

    virtual void readConfigs_async(
        ReadConfigsCb * cb,
        std::vector<std::string> const & tabletNames) = 0;

protected:
    ~ConfigReader() {}
};


#endif // KDI_SERVER_CONFIGREADER_H
