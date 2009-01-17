//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-16
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

#include <kdi/local/disk_table.h>
#include <kdi/local/table_types.h>
#include <warp/options.h>
#include <warp/util.h>
#include <tr1/unordered_set>
#include <iostream>
#include <string>

using namespace kdi;
using namespace warp;
using namespace std;

struct IndexStats
{
    size_t rowSz;
    size_t nRows;
    size_t columnSz;
    size_t nColumns;
    size_t entrySz;
    size_t nEntries;

    IndexStats()
    {
        clear();
    }

    void clear()
    {
        memset(this, 0, sizeof(*this));
    }

    IndexStats const & operator+=(IndexStats const & o)
    {
        rowSz += o.rowSz;
        nRows += o.nRows;
        columnSz += o.columnSz;
        nColumns += o.nColumns;
        entrySz += o.entrySz;
        nEntries += o.nEntries;
        return *this;
    }

    void set(kdi::local::disk::BlockIndexV0 const & idx)
    {
        clear();

        nEntries = idx.blocks.size();
        entrySz = sizeof(idx) + sizeof(idx.blocks[0]) * nEntries;

        std::tr1::unordered_set<StringData const *> rows;
        std::tr1::unordered_set<StringData const *> columns;

        using kdi::local::disk::IndexEntryV0;
        for(IndexEntryV0 const * ent = idx.blocks.begin();
            ent != idx.blocks.end(); ++ent)
        {
            StringData const * row = ent->startKey.row.get();
            if(row && rows.insert(row).second)
            {
                ++nRows;
                rowSz += sizeof(uint32_t) + alignUp(row->size(), 4);
            }

            StringData const * column = ent->startKey.column.get();
            if(column && columns.insert(column).second)
            {
                ++nColumns;
                columnSz += sizeof(uint32_t) + alignUp(column->size(), 4);
            }
        }
    }

    size_t size() const
    {
        return rowSz + columnSz + entrySz;
    }
};

ostream & operator<<(ostream & out, IndexStats const & x)
{
    return out << x.nRows << " rows (" << sizeString(x.rowSz) << "B), "
               << x.nColumns << " columns (" << sizeString(x.columnSz) << "B), "
               << x.nEntries << " entries (" << sizeString(x.entrySz) << "B)";
}

int main(int ac, char ** av)
{
    OptionParser op;

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    IndexStats total;
    size_t dataTot = 0;
    size_t indexTot = 0;

    oort::Record r;
    for(ArgumentList::const_iterator i = args.begin();
        i != args.end(); ++i)
    {
        cout << *i << ": " << flush;
        size_t dataSz = kdi::local::DiskTableV0::loadIndex(*i, r);
        size_t indexSz = r.getLength();
        using kdi::local::disk::BlockIndexV0;
        BlockIndexV0 const * idx = r.as<BlockIndexV0>();
        IndexStats s;
        s.set(*idx);
        cout << sizeString(indexSz) << "/" << sizeString(dataSz)
             << ", " << s << endl;

        total += s;
        dataTot += dataSz;
        indexTot += indexSz;
    }

    cout << "Total: " << sizeString(indexTot) << "/" << sizeString(dataTot)
         << ", " << total
         << ", " << sizeString(indexTot - total.size()) << " left"
         << endl;

    return 0;
}
