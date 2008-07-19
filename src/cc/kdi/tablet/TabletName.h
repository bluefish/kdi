//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/TabletName.h $
//
// Created 2008/07/17
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_TABLET_TABLETNAME_H
#define KDI_TABLET_TABLETNAME_H

#include <string>
#include <warp/interval.h>

namespace kdi {
namespace tablet {

    class TabletName
    {
        std::string tableName;
        warp::IntervalPoint<std::string> lastRow;
        
    public:
        /// Reconstruct a tablet name from an encoded string.
        explicit TabletName(std::string const & encodedName);

        /// Construct a tablet name from a table name and a row upper
        /// bound.
        TabletName(std::string const & tableName,
                   warp::IntervalPoint<std::string> lastRow);

        /// Get the tablet name as a single encoded string.
        std::string getEncoded() const;

        /// Get the table name for the tablet.
        std::string const & getTableName() const
        {
            return tableName;
        }

        /// Get the last row bound for the tablet.
        warp::IntervalPoint<std::string> const & getLastRow() const
        {
            return lastRow;
        }
    };

} // namespace tablet
} // namespace kdi

#endif // KDI_TABLET_TABLETNAME_H
