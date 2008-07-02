//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/sdstore/dynamic_cell.cc#1 $
//
// Created 2007/09/19
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "dynamic_cell.h"

using namespace sdstore;
using namespace warp;

#ifndef NDEBUG

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
    str_data_t getRow(void const * data) const
    {
        DynamicCell const * c = cast(data);
        return str_data_t(c->buf.get(), c->colBegin);
    }

    str_data_t getColumn(void const * data) const
    {
        DynamicCell const * c = cast(data);
        return str_data_t(c->colBegin, c->valBegin);
    }

    str_data_t getValue(void const * data) const
    {
        DynamicCell const * c = cast(data);
        return str_data_t(c->valBegin, c->end);
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
#ifndef NDEBUG
            MProf::get().remove(cell);
#endif
            delete cell;
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
    DynamicCell * data = new DynamicCell(row, column, timestamp, value);

#ifndef NDEBUG
    MProf::get().add(data);
#endif

    return Cell(&interp, data);
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
    str_data_t getRow(void const * data) const
    {
        DynamicCellErasure const * c = cast(data);
        return str_data_t(c->buf.get(), c->colBegin);
    }

    str_data_t getColumn(void const * data) const
    {
        DynamicCellErasure const * c = cast(data);
        return str_data_t(c->colBegin, c->end);
    }

    str_data_t getValue(void const * data) const
    {
        return str_data_t();
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
#ifndef NDEBUG
            MProf::get().remove(cell);
#endif
            delete cell;
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
    DynamicCellErasure * data = new DynamicCellErasure(row, column, timestamp);

#ifndef NDEBUG
    MProf::get().add(data);
#endif

    return Cell(&interp, data);
}
