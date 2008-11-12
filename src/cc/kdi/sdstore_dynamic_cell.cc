//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-09-19
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

#include <kdi/sdstore_dynamic_cell.h>

using namespace sdstore;
using namespace warp;

//#ifndef NDEBUG
//#define REPORT_CELL_STATISTICS
//#endif

#ifdef REPORT_CELL_STATISTICS

#include <warp/strutil.h>
#include <warp/call_queue.h>
#include <boost/thread/mutex.hpp>
#include <boost/bind.hpp>
#include <iostream>
namespace {

    struct MProf
    {
        static MProf & get()
        {
            static MProf m;
            return m;
        }

        size_t nLive;
        size_t nTotal;
        size_t liveSz;
        size_t totalSz;
        boost::mutex mutex;
        CallQueue reporter;

        MProf() :
            nLive(0), nTotal(0), liveSz(0), totalSz(0), reporter(1)
        {
            reporter.callLater(10, boost::bind(&MProf::report, this));
        }

        void report()
        {
            using namespace std;

            boost::mutex::scoped_lock lock(mutex);

            cerr << "Cells: nLive=" << nLive << " nTotal=" << nTotal
                 << " liveSz=" << sizeString(liveSz)
                 << " totalSz=" << sizeString(totalSz)
                 << endl;

            reporter.callLater(10, boost::bind(&MProf::report, this));
        }

        template <class T>
        void add(T const * x)
        {
            boost::mutex::scoped_lock lock(mutex);
            
            ++nLive;
            ++nTotal;
            liveSz += x->size();
            totalSz += x->size();
        }

        template <class T>
        void remove(T const * x)
        {
            boost::mutex::scoped_lock lock(mutex);
            
            --nLive;
            liveSz -= x->size();
        }
    };

}

#endif // NDEBUG

//----------------------------------------------------------------------------
// DynamicCell::Interpreter
//----------------------------------------------------------------------------
class DynamicCell::Interpreter : public CellInterpreter
{
    static DynamicCell const * cast(void const * data)
    {
        return static_cast<DynamicCell const *>(data);
    }

public:
    // Interpreter interface
    StringRange getRow(void const * data) const
    {
        DynamicCell const * c = cast(data);
        return StringRange(c->row, c->col);
    }

    StringRange getColumn(void const * data) const
    {
        DynamicCell const * c = cast(data);
        return StringRange(c->col, c->val);
    }

    StringRange getValue(void const * data) const
    {
        DynamicCell const * c = cast(data);
        return StringRange(c->val, c->end);
    }

    int64_t getTimestamp(void const * data) const
    {
        return cast(data)->timestamp;
    }

    void addRef(void const * data) const
    {
        cast(data)->refCount.increment();
    }

    void release(void const * data) const
    {
        DynamicCell const * cell = cast(data);
        if(cell->refCount.decrementAndTest())
        {
#ifdef REPORT_CELL_STATISTICS
            MProf::get().remove(cell);
#endif
            char const * buf = reinterpret_cast<char const *>(data);
            delete[] buf;
        }
    }
};

//----------------------------------------------------------------------------
// DynamicCell
//----------------------------------------------------------------------------
Cell DynamicCell::make(strref_t row, strref_t column, int64_t timestamp,
                       strref_t value)
{
    static Interpreter interp;

    size_t cellSz = BASE_SIZE + row.size() + column.size() + value.size();
    char * buf = new char[cellSz];
    DynamicCell * cell = reinterpret_cast<DynamicCell *>(buf);

    cell->col = cell->row + row.size();
    cell->val = cell->col + column.size();
    cell->end = cell->val + value.size();
    cell->timestamp = timestamp;
    new (&cell->refCount) warp::AtomicCounter;

    memcpy(cell->row, row.begin(),    row.size());
    memcpy(cell->col, column.begin(), column.size());
    memcpy(cell->val, value.begin(),  value.size());

#ifdef REPORT_CELL_STATISTICS
    MProf::get().add(cell);
#endif

    return Cell(&interp, cell);
}

//----------------------------------------------------------------------------
// DynamicCellErasure::Interpreter
//----------------------------------------------------------------------------
class DynamicCellErasure::Interpreter : public CellInterpreter
{
    static DynamicCellErasure const * cast(void const * data)
    {
        return static_cast<DynamicCellErasure const *>(data);
    }

public:
    // Interpreter interface
    StringRange getRow(void const * data) const
    {
        DynamicCellErasure const * c = cast(data);
        return StringRange(c->row, c->col);
    }

    StringRange getColumn(void const * data) const
    {
        DynamicCellErasure const * c = cast(data);
        return StringRange(c->col, c->end);
    }

    StringRange getValue(void const * data) const
    {
        return StringRange();
    }

    int64_t getTimestamp(void const * data) const
    {
        return cast(data)->timestamp;
    }

    bool isErasure(void const * data) const
    {
        return true;
    }

    void addRef(void const * data) const
    {
        cast(data)->refCount.increment();
    }

    void release(void const * data) const
    {
        DynamicCellErasure const * cell = cast(data);
        if(cell->refCount.decrementAndTest())
        {
#ifdef REPORT_CELL_STATISTICS
            MProf::get().remove(cell);
#endif
            char const * buf = reinterpret_cast<char const *>(data);
            delete[] buf;
        }
    }
};

//----------------------------------------------------------------------------
// DynamicCellErasure
//----------------------------------------------------------------------------
Cell DynamicCellErasure::make(strref_t row, strref_t column,
                              int64_t timestamp)
{
    static Interpreter interp;

    size_t cellSz = BASE_SIZE + row.size() + column.size();
    char * buf = new char[cellSz];
    DynamicCellErasure * cell = reinterpret_cast<DynamicCellErasure *>(buf);

    cell->col = cell->row + row.size();
    cell->end = cell->col + column.size();
    cell->timestamp = timestamp;
    new (&cell->refCount) warp::AtomicCounter;

    memcpy(cell->row, row.begin(),    row.size());
    memcpy(cell->col, column.begin(), column.size());

#ifdef REPORT_CELL_STATISTICS
    MProf::get().add(cell);
#endif

    return Cell(&interp, cell);
}
