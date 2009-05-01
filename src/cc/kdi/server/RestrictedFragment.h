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

#ifndef KDI_SERVER_RESTRICTEDFRAGMENT_H
#define KDI_SERVER_RESTRICTEDFRAGMENT_H

#include <kdi/server/Fragment.h>
#include <warp/interval.h>
#include <boost/noncopyable.hpp>
#include <boost/enable_shared_from_this.hpp>

namespace kdi {
namespace server {

    class RestrictedFragment;

    FragmentCPtr makeRestrictedFragment(
        FragmentCPtr const & base,
        std::vector<std::string> const & families);
 
} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// RestrictedFragment
//----------------------------------------------------------------------------
class kdi::server::RestrictedFragment
    : public kdi::server::Fragment,
      public boost::enable_shared_from_this<RestrictedFragment>,
      private boost::noncopyable
{
public:
    RestrictedFragment(
        FragmentCPtr const & base,
        warp::IntervalSet<std::string> const & columns);

public:
    virtual std::string getFilename() const;
    virtual size_t getDataSize() const;

    virtual void getColumnFamilies(
        std::vector<std::string> & families) const;

    virtual FragmentCPtr getRestricted(
        std::vector<std::string> const & families) const;

    virtual size_t nextBlock(ScanPredicate const & pred,
                             size_t minBlock) const;
    
    virtual std::auto_ptr<FragmentBlock>
    loadBlock(size_t blockAddr) const;

private:
    typedef warp::IntervalSet<std::string> str_iset;

private:
    FragmentCPtr const base;
    str_iset const columns;
};

#endif // KDI_SERVER_RESTRICTEDFRAGMENT_H
