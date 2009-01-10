//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2009-01-09
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

#ifndef KDI_LIMITEDSCANNER_H
#define KDI_LIMITEDSCANNER_H

#include <kdi/cell.h>

namespace kdi {

    class LimitedScanner;

} // namespace kdi


//----------------------------------------------------------------------------
// LimitedScanner
//----------------------------------------------------------------------------
class kdi::LimitedScanner
    : public kdi::CellStream
{
    CellStreamPtr input;
    size_t scanLimit;
    size_t scanSz;
    bool eos;

public:
    LimitedScanner(CellStreamPtr const & input, size_t limit) :
        input(input), scanLimit(limit), scanSz(0), eos(false) {}

    explicit LimitedScanner(size_t limit) :
        scanLimit(limit), scanSz(0), eos(false) {}

    void pipeFrom(CellStreamPtr const & input)
    {
        this->input = input;
        eos = false;
    }

    bool get(Cell & x)
    {
        if(scanSz >= scanLimit)
            return false;

        if(!input)
            return false;

        if((eos = !input->get(x)))
            return false;

        scanSz += ( x.getRow().size()     +
                    x.getColumn().size()  +
                    x.getValue().size()   );
        return true;
    }

    bool fetch()
    {
        scanSz = 0;
        return input && !eos;
    }

    bool endOfStream() const { return eos; }
};


#endif // KDI_LIMITEDSCANNER_H
