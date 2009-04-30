//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-17
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

#include <kdi/server/RestrictedFragment.h>
#include <kdi/scan_predicate.h>
#include <kdi/server/misc_util.h>

using namespace kdi;
using namespace kdi::server;

//----------------------------------------------------------------------------
// RestrictedFragment
//----------------------------------------------------------------------------
RestrictedFragment::RestrictedFragment(
    FragmentCPtr const & base,
    warp::IntervalSet<std::string> const & columns) :
    base(base),
    columns(columns)
{
}

std::string RestrictedFragment::getFilename() const
{
    return base->getFilename();
}

void RestrictedFragment::getColumnFamilies(
    std::vector<std::string> & families) const
{
    families.clear();
    for(str_iset::const_iterator i = columns.begin();
        i != columns.end(); i += 2)
    {
        std::string const & s = i->getValue();
        families.push_back(s.substr(0, s.size()-1));
    }
}

FragmentCPtr RestrictedFragment::getRestricted(
    std::vector<std::string> const & families) const
{
    str_iset requested = getFamilyColumnSet(families);
    requested.intersect(columns);
        
    //if(requested == columns)
    //    return shared_from_this();

    FragmentCPtr p(new RestrictedFragment(base, requested));
    return p;
}

size_t RestrictedFragment::nextBlock(ScanPredicate const & pred,
                                     size_t minBlock) const
{
    ScanPredicate p2(pred);
    if(ScanPredicate::StringSetCPtr cols = pred.getColumnPredicate())
    {
        p2.setColumnPredicate(
            str_iset(columns).intersect(*cols));
    }
    else
    {
        p2.setColumnPredicate(columns);
    }
    return base->nextBlock(p2, minBlock);
}
    
std::auto_ptr<FragmentBlock>
RestrictedFragment::loadBlock(size_t blockAddr) const
{
    return base->loadBlock(blockAddr);
}

//----------------------------------------------------------------------------
// makeRestrictedFragment
//----------------------------------------------------------------------------
FragmentCPtr kdi::server::makeRestrictedFragment(
    FragmentCPtr const & base,
    std::vector<std::string> const & families)
{
    std::vector<std::string> baseFamilies;
    base->getColumnFamilies(baseFamilies);
    warp::IntervalSet<std::string> cols = getFamilyColumnSet(baseFamilies);
    cols.intersect(getFamilyColumnSet(families));

    FragmentCPtr p(new RestrictedFragment(base, cols));
    return p;
}
