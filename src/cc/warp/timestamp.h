//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-03-21
// 
// This file is part of the warp library.
// 
// The warp library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The warp library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef WARP_TIMESTAMP_H
#define WARP_TIMESTAMP_H

#include <warp/string_range.h>
#include <stdint.h>
#include <iostream>

namespace warp {

    /// A representation of a point in time.  This is a thin wrapper
    /// around a 64-bit integer.  Its purpose is to define a standard
    /// notion of a timestamp represented as an integer, since there
    /// are about a dozen competing ways to do so.  This notion of a
    /// timestamp is that the integer represents the number of
    /// microseconds that have elapsed since 1970-01-01T00:00:00Z.
    class Timestamp;

} // namespace warp

//----------------------------------------------------------------------------
// Timestamp
//----------------------------------------------------------------------------
class warp::Timestamp
{
    // Time measured in microseconds since 1970-01-01T00:00:00Z.
    int64_t usec;

    explicit Timestamp(int64_t usec) : usec(usec) {}

public:
    Timestamp() : usec(0) {}

    /// Parse the given string into a Timestamp.  The string may
    /// either be an ISO8601 datetime, or an integer literal prefixed
    /// with an '@'.
    explicit Timestamp(strref_t s);

    /// Set the timestamp from broken down parts.  The values passed
    /// in should correspond to printable values in the ISO8601
    /// representation.  The timezone parameter, gmtoff, should be
    /// given as the second offset from GMT to the input timezone.
    void set(int year, int month, int day,
             int hour, int min, int sec,
             int32_t usec, int32_t gmtoff);

    /// Set the timestamp from broken down parts in the local
    /// timezone.
    void setLocal(int year, int month, int day,
                  int hour, int min, int sec,
                  int32_t usec);

    /// Set the timestamp from broken down parts in the UTC timezone.
    void setUtc(int year, int month, int day,
                int hour, int min, int sec,
                int32_t usec);

    operator int64_t() const { return usec; }

    bool operator< (Timestamp const & o) const { return usec <  o.usec; }
    bool operator<=(Timestamp const & o) const { return usec <= o.usec; }
    bool operator> (Timestamp const & o) const { return usec >  o.usec; }
    bool operator>=(Timestamp const & o) const { return usec >= o.usec; }
    bool operator==(Timestamp const & o) const { return usec == o.usec; }
    bool operator!=(Timestamp const & o) const { return usec != o.usec; }

    Timestamp operator+(Timestamp const & o) const {
        return Timestamp(usec + o.usec);
    }
    Timestamp operator-(Timestamp const & o) const {
        return Timestamp(usec - o.usec);
    }
    Timestamp & operator+=(Timestamp const & o) {
        usec += o.usec; return *this;
    }
    Timestamp & operator-=(Timestamp const & o) {
        usec -= o.usec; return *this;
    }

    /// Get a Timestamp for the current time.
    static Timestamp now();

    /// Parse the given string into a Timestamp.  The string may
    /// either be an ISO8601 datetime, or an integer literal prefixed
    /// with an '@'.
    static Timestamp fromString(strref_t s) {
        return Timestamp(s);
    }

    /// Get a Timestamp for the given seconds-from-Epoch time.
    static Timestamp fromSeconds(double s) {
        return Timestamp(int64_t(s * 1e6));
    }

    /// Get a Timestamp for the given seconds-from-Epoch time.
    static Timestamp fromSeconds(int64_t s) {
        return Timestamp(s * 1000000);
    }

    /// Get a Timestamp for the given milliseconds-from-Epoch time.
    static Timestamp fromMilliseconds(double ms) {
        return Timestamp(int64_t(ms * 1e3));
    }

    /// Get a Timestamp for the given milliseconds-from-Epoch time.
    static Timestamp fromMilliseconds(int64_t ms) {
        return Timestamp(ms * 1000);
    }

    /// Get a Timestamp for the given microseconds-from-Epoch time.
    static Timestamp fromMicroseconds(double us) {
        return Timestamp(int64_t(us));
    }

    /// Get a Timestamp for the given microseconds-from-Epoch time.
    static Timestamp fromMicroseconds(int64_t us) {
        return Timestamp(us);
    }
};

namespace warp
{
    // Output as an ISO8601 datetime string
    std::ostream & operator<<(std::ostream & o, Timestamp const & t);
}


#endif // WARP_TIMESTAMP_H
