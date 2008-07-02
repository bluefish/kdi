//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/oort/partmgr.h#1 $
//
// Created 2006/02/21
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef OORT_PARTMGR_H
#define OORT_PARTMGR_H

#include "record.h"
#include "typeregistry.h"
#include "warp/singleton.h"

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
