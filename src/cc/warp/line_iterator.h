//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-01-19
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

#ifndef WARP_LINE_ITERATOR_H
#define WARP_LINE_ITERATOR_H

#include <warp/strutil.h>
#include <string>

namespace warp
{
    /// Interpret a character buffer as sequence of CR, LF, or CRLF
    /// delimited lines.  The delimiter is determined independently
    /// for each line in the sequence.
    class LineIterator;
}

//----------------------------------------------------------------------------
// LineIterator
//----------------------------------------------------------------------------
class warp::LineIterator
{
    char const * bufferEnd;
    char const * lineEnd;

public:
    /// Initialize a LineIterator from a character range.
    LineIterator(char const * begin, char const * end) :
        bufferEnd(end), lineEnd(begin) {}

    /// Initialize a LineIterator from a pointer and length.
    LineIterator(char const * begin, size_t len) :
        bufferEnd(begin + len), lineEnd(begin) {}

    /// Get the next line in the buffer.  If \c strip is true, strip
    /// the trailing whitespace in the returned line.  Otherwise
    /// include it.
    /// @return true if the sequence contained another line, false at
    /// the end of the buffer
    bool get(StringRange & x, bool strip)
    {
        // Check to see if we're at the end of the buffer
        if(lineEnd == bufferEnd)
            return false;

        // Set beginning of next line at the end of the last one
        char const * lineBegin = lineEnd;

        // Find the line-ending delimiter (end of the stripped line)
        char const * stripEnd = lineEnd;
        for(; stripEnd != bufferEnd; ++stripEnd)
        {
            if(*stripEnd == '\n' || *stripEnd == '\r')
                break;
        }

        // Find the unstripped end of the line and update the lineEnd
        // pointer to that
        switch(bufferEnd - stripEnd)
        {
            case 0:
                // End of buffer
                lineEnd = stripEnd;
                break;

            case 1:
                // CR or LF is last char in buffer
                lineEnd = stripEnd + 1;
                break;

            default:
                // There's at least 2 characters after stripEnd.  See
                // if this is a CRLF line ending.
                if(stripEnd[0] == '\r' && stripEnd[1] == '\n')
                    lineEnd = stripEnd + 2;
                else
                    lineEnd = stripEnd + 1;
                break;
        }

        // Set the result and return.  Either include the ending
        // delimiter or not depending on the strip setting.
        x = StringRange(lineBegin, strip ? stripEnd : lineEnd);
        return true;
    }

    char const *position() {
        return lineEnd;
    }

    /// Get the next line in the buffer and copy it into a string.  If
    /// \c strip is true, strip the trailing whitespace in the
    /// returned line.  Otherwise include it.
    /// @return true if the sequence contained another line, false at
    /// the end of the buffer
    bool get(std::string & x, bool strip)
    {
        StringRange s;
        if(!get(s, strip))
            return false;

        x.assign(s.begin(), s.end());
        return true;
    }
};

#endif // WARP_LINE_ITERATOR_H
