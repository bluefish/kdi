//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/server/Fragment.h $
//
// Created 2009/02/26
//
// Copyright 2009 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_SERVER_DISK_FRAGMENT_H
#define KDI_SERVER_DISK_FRAGMENT_H

#include <warp/file.h>
#include <warp/Iterator.h>
#include <warp/string_range.h>
#include <memory>
#include <stddef.h>

#include <kdi/scan_predicate.h>
#include <kdi/server/Fragment.h>
#include <kdi/local/table_types.h>
#include <kdi/local/index_cache.h>
#include <oort/fileio.h>

namespace kdi {
namespace server {

    class DiskFragment;
    class DiskBlock;
    class DiskBlockReader;

    typedef std::auto_ptr<DiskFragment> DiskFragmentPtr;
    typedef std::auto_ptr<DiskBlock> DiskBlockPtr;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// DiskFragment
//----------------------------------------------------------------------------
class kdi::server::DiskFragment
    : public kdi::server::Fragment
{
    warp::FilePtr fp;
    oort::FileInput::handle_t input;
    kdi::local::CacheRecord indexRec;

public:
    explicit DiskFragment(std::string const & fn);

    virtual size_t nextBlock(ScanPredicate const & pred,
                             size_t minBlock) const;
    
    virtual std::auto_ptr<FragmentBlock>
    loadBlock(size_t blockAddr) const;
};

//----------------------------------------------------------------------------
// DiskBlock
//----------------------------------------------------------------------------
class kdi::server::DiskBlock
    : public kdi::server::FragmentBlock
{
    oort::Record blockRec;

public:
    DiskBlock(warp::FilePtr const & fp,
              kdi::local::disk::IndexEntryV1 const & idx);

    virtual ~DiskBlock();
    virtual std::auto_ptr<FragmentBlockReader>
    makeReader(ScanPredicate const & pred) const; 
};

//----------------------------------------------------------------------------
// DiskBlockReader
//----------------------------------------------------------------------------
class kdi::server::DiskBlockReader
    : public kdi::server::FragmentBlockReader
{
    oort::Record blockRec;
    ScanPredicate pred;
    kdi::marshal::CellBlock const * block;
    kdi::marshal::CellData const * cellIt;
    kdi::marshal::CellData const * cellEnd;

public:
    DiskBlockReader(oort::Record const & r, ScanPredicate const & pred);
    
    virtual bool advance(CellKey & nextKey);
    virtual void copyUntil(const kdi::CellKey * stopKey, 
                           bool filterErasures,
                           kdi::server::CellBuilder & out);
};


#endif // KDI_SERVER_DISK_FRAGMENT_H