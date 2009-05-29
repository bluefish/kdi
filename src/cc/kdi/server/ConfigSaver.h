//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-26
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

#ifndef KDI_SERVER_CONFIGSAVER_H
#define KDI_SERVER_CONFIGSAVER_H

#include <warp/QueuedWorker.h>
#include <kdi/server/TabletConfig.h>

namespace kdi {
namespace server {

    class ConfigSaverWork;
    class ConfigSaver;

    // Forward declarations
    class ConfigWriter;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// ConfigSaverWork
//----------------------------------------------------------------------------
class kdi::server::ConfigSaverWork
{
public:
    ConfigSaverWork() {}
    explicit ConfigSaverWork(TabletConfigVecCPtr const & configs) :
        configs(configs) {}

    void setConfigs(TabletConfigVecCPtr const & configs)
    {
        this->configs = configs;
    }

    TabletConfigVecCPtr const & getConfigs() const
    {
        return configs;
    }

public:
    virtual void done() = 0;

protected:
    ~ConfigSaverWork() {}

private:
    TabletConfigVecCPtr configs;
};


//----------------------------------------------------------------------------
// ConfigSaver
//----------------------------------------------------------------------------
class kdi::server::ConfigSaver
    : public warp::QueuedWorker<ConfigSaverWork *>
{
    typedef warp::QueuedWorker<ConfigSaverWork *> super;

public:
    explicit ConfigSaver(ConfigWriter * writer);

protected:                      // QueuedWorker API
    virtual void processWork(super::work_cref_t x);

private:
    ConfigWriter * const writer;
};

#endif // KDI_SERVER_CONFIGSAVER_H
