//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-11
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

#ifndef KDI_SERVER_SERIALIZER_H
#define KDI_SERVER_SERIALIZER_H

#include <warp/PersistentWorker.h>
#include <kdi/scan_predicate.h>
#include <kdi/server/FragmentWriter.h>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <vector>
#include <string>
#include <memory>

namespace kdi {
namespace server {

    class Serializer;

    // Forward declarations
    class Fragment;
    class PendingFile;
    typedef boost::shared_ptr<Fragment const> FragmentCPtr;
    typedef boost::shared_ptr<PendingFile const> PendingFileCPtr;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// Serializer
//----------------------------------------------------------------------------
class kdi::server::Serializer :
    public warp::PersistentWorker
{
public:
    struct Work
    {
        std::vector<FragmentCPtr> fragments;
        ScanPredicate predicate;
        boost::scoped_ptr<FragmentWriter> output;

        virtual ~Work() {}
        virtual void done(std::vector<std::string> const & rowCoverage,
                          PendingFileCPtr const & outFn) = 0;
    };

    class Input
    {
    public:
        virtual std::auto_ptr<Work> getWork() = 0;
    protected:
        ~Input() {}
    };

public:
    explicit Serializer(Input * input) :
        PersistentWorker("Serialize"),
        input(input)
    {
    }

protected:
    virtual bool doWork();

private:
    Input * const input;
};

#endif // KDI_SERVER_SERIALIZER_H
