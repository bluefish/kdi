//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-09-06
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

#ifndef KDI_SCAN_PREDICATE_H
#define KDI_SCAN_PREDICATE_H

#include <warp/interval.h>
#include <warp/string_range.h>
#include <string>
#include <iostream>
#include <boost/shared_ptr.hpp>

namespace kdi {
    
    class ScanPredicate;

} // namespace kdi


//----------------------------------------------------------------------------
// ScanPredicate
//----------------------------------------------------------------------------
class kdi::ScanPredicate
{
public:
    typedef boost::shared_ptr<warp::IntervalSet<std::string> const> StringSetCPtr;
    typedef boost::shared_ptr<warp::IntervalSet<int64_t> const> TimestampSetCPtr;
    typedef ScanPredicate my_t;

private:
    StringSetCPtr     rows;
    StringSetCPtr     columns;
    TimestampSetCPtr  timestamps;
    int               maxHistory;

public:
    /// Create a default ScanPredicate, which includes all cells in
    /// the scan.
    ScanPredicate() : maxHistory(0) {}

    /// Create a ScanPredicate from an expression string.  The
    /// expression grammar is:
    ///
    ///   PREDICATE    := ( SUBPREDICATE ( 'and' SUBPREDICATE )* )?
    ///   SUBPREDICATE := INTERVALSET | HISTORY
    ///   INTERVALSET  := INTERVAL ( 'or' INTERVAL )*
    ///   INTERVAL     := ( LITERAL REL-OP )? FIELD REL-OP LITERAL
    ///   HISTORY      := 'history' '=' INTEGER
    ///   LITERAL      := STRING-LITERAL | TIME-LITERAL
    ///   TIME-LITERAL := ISO8601-DATETIME | ( '@' INTEGER )
    ///   FIELD        := 'row' | 'column' | 'time'
    ///   REL-OP       := '<' | '<=' | '>' | '>=' | '=' | '~='
    ///
    /// The STRING-LITERALs may be double- or single-quoted strings.
    /// The following backslash escape sequences are supported:
    ///   - C-style: \n,\r,\t,\f,\a,\v,\\,\",\'
    ///   - Hex: \xXX, where each 'X' is a hex character
    ///   - Unicode: \uXXXX, where each 'X' is a hex character
    ///   - Unicode: \UXXXXXXXX, where each 'X' is a hex character
    /// C and Hex escapes will be replaced with their raw byte value.
    /// Unicode escapes will be replaced with the UTF-8 encoding of
    /// the character.
    ///
    /// The familiar REL-OPs all have the usual interpretation.  The
    /// odd one, '~=', is a 'starts-with' operator.  For example:
    ///     column ~= "group:"
    /// means that columns starting with "group:" should be selected.
    ///
    /// The grammar is approximate.  Here are some restrictions:
    ///   - different FIELDs cannot be mixed in the same INTERVALSET
    ///   - there can be only one INTERVALSET per FIELD
    ///   - there can be only one HISTORY
    ///   - STRING-LITERALS can only be used with 'row' and 'column'
    ///   - TIME-LITERALS can only be used with 'time'
    ///   - the '~=' operator only works with STRING-LITERALs
    ///   - the extended INTERVAL form only works when the two REL-OPs
    ///     are inequalities in the same direction (e.g. both '<' or
    ///     '<=', or both '>' or '>=')
    ///
    /// ISO8601-DATETIMEs will be converted to an integer representing
    /// the number of microseconds from 1970-01-01T00:00:00.000000Z to
    /// the given timestamp.  If the timezone is omitted from the
    /// given time, it assumed to be in local time.
    ///
    /// Predicate examples:
    ///    row = 'com.foo.www/index.html' and history = 1
    ///    row ~= 'com.foo' and time >= 1999-01-02T03:04:05.678901Z
    ///    "word:cat" < column <= "word:dog" or column >= "word:fish"
    ///    time = @0
    ///
    /// @throws ValueError if the expression cannot be parsed
    explicit ScanPredicate(warp::strref_t expr);

    /// Return true if this predicate is more restrictive than a full
    /// cell scan.  The default predicate is unconstrained.  Filtering
    /// constraints are specified by calling the "set" methods.
    bool isConstrained() const
    {
        return rows || columns || timestamps || maxHistory;
    }

    /// Restrict the scan to the given set of rows.  Only cells with
    /// row names contained in the given interval set will be returned
    /// by the scan.  The default behavior is to return all cells.
    my_t & setRowPredicate(warp::IntervalSet<std::string> const & pred)
    {
        rows.reset(new warp::IntervalSet<std::string>(pred));
        return *this;
    }

    /// Restrict the scan to the given set of columns.  Only cells
    /// with column names contained in the given interval set will be
    /// returned by the scan.  The default behavior is to return all
    /// cells.
    my_t & setColumnPredicate(warp::IntervalSet<std::string> const & pred)
    {
        columns.reset(new warp::IntervalSet<std::string>(pred));
        return *this;
    }

    /// Restrict the scan to the given set of timestamps.  Only cells
    /// with timestamps contained in the given interval set will be
    /// returned by the scan.  The default behavior is to return all
    /// cells.
    my_t & setTimePredicate(warp::IntervalSet<int64_t> const & pred)
    {
        timestamps.reset(new warp::IntervalSet<int64_t>(pred));
        return *this;
    }

    /// Set the maximum history length in the scan.  Cells in the scan
    /// have unique (row, column, timestamp) keys, but there may be
    /// many cells with the same row and column but different
    /// timestamps.  These can be interpreted as historical context
    /// for a cell value.  The maximum history parameter restricts the
    /// scan to the latest \c K timestamps for each unique (row,
    /// column) pair.  A negative value for \c K means that everything
    /// *except* the latest \c K timestamps are returned.  Setting \c
    /// K to zero means return all timestamps.  This is the default
    /// behavior.  Note that this predicate is applied as filter
    /// *after* the timestamp interval set predicate.
    my_t & setMaxHistory(int k)
    {
        maxHistory = k;
        return *this;
    }

    /// Clear the row constraint.
    my_t & clearRowPredicate()
    {
        rows.reset();
        return *this;
    }

    /// Clear the column constraint.
    my_t & clearColumnPredicate()
    {
        columns.reset();
        return *this;
    }

    /// Clear the time constraint.
    my_t & clearTimePredicate()
    {
        timestamps.reset();
        return *this;
    }

    /// Get the interval set of rows we're interested in.  The
    /// returned value may be null, which means we want ALL rows.
    StringSetCPtr getRowPredicate() const { return rows; }

    /// Get the interval set of columns we're interested in.  The
    /// returned value may be null, which means we want ALL columns.
    StringSetCPtr getColumnPredicate() const { return columns; }

    /// Get the interval set of timestamps we're interested in.  The
    /// returned value may be null, which means we want ALL
    /// timestamps.
    TimestampSetCPtr getTimePredicate() const { return timestamps; }

    /// Get the maximum history length to allow in the scan.  A
    /// positive return value, \c K, means we only want the latest \c
    /// K revisions of each (row, column) pair in the scan.  A
    /// negative value means we want all but the latest \c K
    /// revisions.  A return value of zero means we want all
    /// revisions.
    int getMaxHistory() const { return maxHistory; }

    /// Return a copy of this ScanPredicate with the row predicate
    /// clipped to the given span.
    ScanPredicate clipRows(warp::Interval<std::string> const & span) const;

    /// If this predicate has a column restriction, get the set of
    /// column families involved.  The columns involved in the
    /// predicate may be more restricted than the returned set of
    /// column families, but the families may be used as filter.  If
    /// the predicate has no column restriction or the restriction
    /// cannot be distilled to a discrete set of column families, the
    /// function will return false, indicating that no useful column
    /// family filter is available.  Note that the ranges in the
    /// returned vector share the lifetime of this predicates column
    /// predicate.
    /// @returns true iff a containing set of column families can be
    /// inferred from the column predicate
    bool getColumnFamilies(std::vector<warp::StringRange> & families) const;

    /// Return true if the predicate contains the given row.
    bool containsRow(warp::strref_t row) const
    {
        return !rows || rows->contains(row);
    }

    /// Return true if the predicate contains the given column.
    bool containsColumn(warp::strref_t column) const
    {
        return !columns || columns->contains(column);
    }

    /// Return true if the predicate contains the given timestamp.
    bool containsTimestamp(int64_t timestamp) const
    {
        return !timestamps || timestamps->contains(timestamp);
    }
};

namespace kdi {

    /// Output scan predicate as an expression
    std::ostream & operator<<(std::ostream & out, ScanPredicate const & pred);

} // namespace kdi

#endif // KDI_SCAN_PREDICATE_H
