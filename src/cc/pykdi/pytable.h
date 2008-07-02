//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/pykdi/pytable.h#1 $
//
// Created 2008/01/11
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef PYKDI_PYTABLE_H
#define PYKDI_PYTABLE_H

#include <kdi/table.h>
#include <string>

namespace pykdi {

    class PyTable;

    // Forward declaration
    class PyScan;

} // namespace pykdi


//----------------------------------------------------------------------------
// PyTable
//----------------------------------------------------------------------------
class pykdi::PyTable
{
    std::string uri;
    kdi::TablePtr table;

public:
    explicit PyTable(std::string const & uri);

    void set(std::string const & row, std::string const & col,
             int64_t rev, std::string const & val);
    void erase(std::string const & row, std::string const & col,
               int64_t rev);

    void sync();

    PyScan scan() const;
    PyScan scan(kdi::ScanPredicate const & pred) const;
    PyScan scan(std::string const & pred) const;

public:
    static void defineWrapper();
};

#endif // PYKDI_PYTABLE_H
