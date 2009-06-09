//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-20
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

#ifndef KDI_SERVER_TESTFRAGMENT_H
#define KDI_SERVER_TESTFRAGMENT_H

#include <kdi/server/Fragment.h>
#include <kdi/server/RestrictedFragment.h>
#include <kdi/CellKey.h>
#include <boost/enable_shared_from_this.hpp>
#include <boost/noncopyable.hpp>
#include <vector>
#include <string>
#include <set>

namespace kdi {
namespace server {

    class TestFragment;

    typedef boost::shared_ptr<TestFragment> TestFragmentPtr;
    typedef boost::shared_ptr<TestFragment const> TestFragmentCPtr;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// TestFragment
//----------------------------------------------------------------------------
class kdi::server::TestFragment
    : public kdi::server::Fragment,
      public boost::enable_shared_from_this<TestFragment>,
      private boost::noncopyable
{
public:                         // Fragment API
    virtual std::string getFilename() const { return name; }

    virtual size_t getDataSize() const { return dataSz; }

    virtual size_t getPartialDataSize(
        warp::Interval<std::string> const & rows) const
    {
        return getDataSize();
    }

    virtual void getColumnFamilies(
        std::vector<std::string> & families) const
    {
        families.clear();
        families.insert(families.end(),
                        colFamilies.begin(),
                        colFamilies.end());
    }

    virtual FragmentCPtr getRestricted(
        std::vector<std::string> const & families) const
    {
        return makeRestrictedFragment(shared_from_this(), families);
    }

    virtual size_t nextBlock(ScanPredicate const & pred,
                             size_t minBlock) const
    {
        if(minBlock < blocks.size())
            return minBlock;
        return size_t(-1);
    }
    
    virtual std::auto_ptr<FragmentBlock>
    loadBlock(size_t blockAddr) const
    {
        std::auto_ptr<FragmentBlock> p(new BlockBlock(blocks[blockAddr]));
        return p;
    }

public:
    TestFragment() { init("test"); }
    explicit TestFragment(std::string const & name) { init("test"); }

    ~TestFragment()
    {
        std::for_each(blocks.begin(), blocks.end(), warp::delete_ptr());
    }

    void set(strref_t r, strref_t c, int64_t t, strref_t v)
    {
        lastBlock().cells.push_back(Cell(r,c,t,v));
        ++nCells;
        dataSz += lastBlock().cells.back().size();
        colFamilies.insert(getFamily(c));
    }

    void erase(strref_t r, strref_t c, int64_t t)
    {
        lastBlock().cells.push_back(Cell(r,c,t));
        ++nCells;
        dataSz += lastBlock().cells.back().size();
        colFamilies.insert(getFamily(c));
    }
    
    void startNewBlock()
    {
        blocks.push_back(0);
        blocks.back() = new Block;
    }

private:
    std::string getFamily(strref_t c) const
    {
        std::string r;
        char const * sep = std::find(c.begin(), c.end(), ':');
        if(sep != c.end())
            r.assign(c.begin(), sep);
        return r;
    }

    struct Cell
    {
        CellKey key;
        std::string val;
        bool erasure;

        Cell() {}

        Cell(strref_t r, strref_t c, int64_t t, strref_t v)
        {
            key.set(r,c,t);
            v.assignTo(val);
            erasure = false;
        }

        Cell(strref_t r, strref_t c, int64_t t)
        {
            key.set(r,c,t);
            erasure = true;
        }

        size_t size() const
        {
            return sizeof(*this) + key.getRow().size() +
                key.getColumn().size() + val.size();
        }
    };

    struct Block
    {
        std::vector<Cell> cells;
    };

    class BlockReader
        : public FragmentBlockReader
    {
        Block * block;
        ScanPredicate pred;
        size_t idx;

    public:
        BlockReader(Block * block, ScanPredicate const & pred) :
            block(block), pred(pred), idx(0) {}

    public:                     // FragmentBlockReader
        virtual bool advance(CellKey & nextKey)
        {
            if(idx >= block->cells.size() ||
               ++idx >= block->cells.size())
            {
                return false;
            }

            nextKey = block->cells[idx].key;
            return true;
        }

        virtual void copyUntil(CellKey const * stopKey, CellOutput & out)
        {
            for(; idx < block->cells.size(); ++idx)
            {
                Cell const & x = block->cells[idx];

                if(stopKey && !(x.key < *stopKey))
                    break;

                if(!pred.containsRow(x.key.getRow()) ||
                   !pred.containsColumn(x.key.getColumn()) ||
                   !pred.containsTimestamp(x.key.getTimestamp()))
                    continue;

                if(x.erasure)
                    out.emitErasure(x.key.getRow(), x.key.getColumn(), x.key.getTimestamp());
                else
                    out.emitCell(x.key.getRow(), x.key.getColumn(), x.key.getTimestamp(), x.val);
            }
        }
    };

    class BlockBlock
        : public FragmentBlock
    {
        Block * block;

    public:
        BlockBlock(Block * block) : block(block) {}

    public:                     // FragmentBlock
        virtual std::auto_ptr<FragmentBlockReader>
        makeReader(ScanPredicate const & pred) const
        {
            std::auto_ptr<FragmentBlockReader> r(
                new BlockReader(block, pred));
            return r;
        }
    };

    void init(std::string const & n)
    {
        name = n;
        startNewBlock();
        nCells = 0;
        dataSz = 0;
    }

    Block & lastBlock() const
    {
        return *blocks.back();
    }

    std::string name;
    std::vector<Block *> blocks;
    std::set<std::string> colFamilies;
    size_t nCells;
    size_t dataSz;
};

#endif // KDI_SERVER_TESTFRAGMENT_H
