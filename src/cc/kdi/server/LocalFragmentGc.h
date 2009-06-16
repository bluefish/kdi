//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-06-15
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

#ifndef KDI_SERVER_LOCALFRAGMENTGC_H
#define KDI_SERVER_LOCALFRAGMENTGC_H

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <tr1/unordered_set>
#include <string>

namespace kdi {
namespace server {

    class LocalFragmentGc;

    // Forward declarations
    class FragmentRemover;
    class Fragment;
    typedef boost::shared_ptr<Fragment const> FragmentCPtr;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// LocalFragmentGc
//----------------------------------------------------------------------------
class kdi::server::LocalFragmentGc
    : private boost::noncopyable
{
public:
    explicit LocalFragmentGc(FragmentRemover * remover);

    /// Wrap a Fragment so that its backing file will automatically be
    /// deleted when the wrapped handle is destroyed.  Wrapped
    /// Fragments can escape this fate by being "exported" later.
    /// Exported fragment files will be left to a global garbage
    /// collection process.
    FragmentCPtr wrapLocalFragment(FragmentCPtr const & fragment);
    
    /// Export a previously wrapped Fragment.  Once it has been
    /// exported, it will not be automatically deleted.
    void exportFragment(FragmentCPtr const & fragment);

    /// Mark all known Fragments for export.  This can be used at
    /// server shutdown.
    void exportAll();

private:
    class Wrapper;
    typedef std::tr1::unordered_set<std::string> set_t;

private:
    void release(std::string const & filename);

private:
    FragmentRemover * const remover;
    set_t localSet;
    boost::mutex mutex;
};

#endif // KDI_SERVER_LOCALFRAGMENTGC_H
