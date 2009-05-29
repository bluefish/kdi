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

#include <kdi/server/ConfigSaver.h>
#include <kdi/server/ConfigWriter.h>
#include <cassert>

using namespace kdi::server;

//----------------------------------------------------------------------------
// ConfigSaver
//----------------------------------------------------------------------------
ConfigSaver::ConfigSaver(ConfigWriter * writer) :
    super("Config"), writer(writer)
{
}

void ConfigSaver::processWork(super::work_cref_t x)
{
    assert(x);
    
    if(writer && x->getConfigs())
        writer->writeConfigs(*x->getConfigs());

    x->done();
}
