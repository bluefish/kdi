//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-02-21
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef OORT_PARTMGR_H
#define OORT_PARTMGR_H

#include <oort/record.h>
#include <oort/typeregistry.h>
#include <warp/singleton.h>

#include <boost/utility.hpp>
#include <string>
#include <map>
#include <utility>

namespace oort
{
    typedef uint32_t (PartitionFunc)(Record const &, uint32_t);

    class PartitionManager;
    class Partitioner;
}


//----------------------------------------------------------------------------
// PartitionManager
//----------------------------------------------------------------------------
class oort::PartitionManager : public TypeRegistry<PartitionFunc *>,
                               public warp::Singleton<PartitionManager>
{
    friend class warp::Singleton<PartitionManager>;
    PartitionManager() :
        TypeRegistry<PartitionFunc *>("oort::PartitionManager")
    {
    }

    template <class T> struct RegHelper
    {
        static uint32_t getPartition(Record const & r, uint32_t nParts)
        {
            return r.cast<T>()->getPartition(nParts);
        }
    };

public:
    template <class T> void regPartition()
    {
        registerType<T>(&RegHelper<T>::getPartition);
    }
};


//----------------------------------------------------------------------------
// Partitioner
//----------------------------------------------------------------------------
class oort::Partitioner
{
    mutable PartitionFunc * partFunc;
    mutable uint32_t partType;
    mutable uint32_t partVer;
    uint32_t nParts;
    
public:
    explicit Partitioner(uint32_t nParts) :
        partFunc(0),
        partType(0),
        partVer(0),
        nParts(nParts)
    {
    }

    uint32_t operator()(Record const & r) const
    {
        uint32_t rtype = r.getType();
        uint32_t rver = r.getVersion();
        if(!partFunc || rtype != partType || rver != partVer)
        {
            partFunc = PartitionManager::get().lookupType(rtype, rver);
            partType = rtype;
            partVer = rver;
        }
        return (*partFunc)(r, nParts);
    }
};


#endif // OORT_PARTMGR_H
