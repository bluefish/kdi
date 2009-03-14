//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-02-27
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

#ifndef KDI_SERVER_TABLE_H
#define KDI_SERVER_TABLE_H

#include <kdi/server/Fragment.h>
#include <kdi/server/CommitRing.h>
#include <warp/interval.h>
#include <boost/thread/mutex.hpp>
#include <string>
#include <vector>
#include <memory>

namespace kdi {
namespace server {

    class Table;

    // Forward declarations
    class CellBuffer;
    class FragmentEventListener;
    class TabletEventListener;

} // namespace server

    // Forward declarations
    class ScanPredicate;

} // namespace kdi

//----------------------------------------------------------------------------
// Table
//----------------------------------------------------------------------------
class kdi::server::Table
{
public:
    boost::mutex tableMutex;

public:
    struct IsNameChar
    {
        inline bool operator()(char c) const
        {
            return ( ('a' <= c && c <= 'a') ||
                     ('A' <= c && c <= 'Z') ||
                     ('0' <= c && c <= '9') ||
                     c == '/' ||
                     c == '-' ||
                     c == '_' );
        }
    };

public:
    /// Make sure that the tablets containing all the given rows are
    /// currently loaded in the table.
    void verifyTabletsLoaded(std::vector<warp::StringRange> const & rows) const;
    
    /// Make sure that all of the given rows have a commit number less
    /// than or equal to maxTxn.
    void verifyCommitApplies(std::vector<warp::StringRange> const & rows,
                             int64_t maxTxn) const;

    /// Update the committed transaction number for the given rows.
    void updateRowCommits(std::vector<warp::StringRange> const & rows,
                          int64_t commitTxn);

    void addFragmentListener(FragmentEventListener * listener);
    void removeFragmentListener(FragmentEventListener * listener);

    void addTabletListener(TabletEventListener * listener);
    void removeTabletListener(TabletEventListener * listener);

    void triggerNewFragmentEvent(Fragment const * frag);

    /// Get the last transaction committed to this Table.
    int64_t getLastCommitTxn() const;

    /// Get the ordered chain of fragments to merge for the first part
    /// of the given predicate.  Throws and error if such a chain is
    /// not available (maybe the tablet corresponding to the first
    /// part of the range is not loaded on this server).  When the
    /// tablet is available, the fragment chain is returned in proper
    /// merge order.  The row interval over which the chain is valid
    /// is also returned.
    void getFirstFragmentChain(ScanPredicate const & pred,
                               std::vector<Fragment const *> & chain,
                               warp::Interval<std::string> & rows) const;

    void addMemoryFragment(FragmentCPtr const & p, size_t sz)
    {
        memFrags.addFragment(p, sz);
    }

private:
    class Tablet;
    class TabletLt;

    typedef std::vector<Tablet *> tablet_vec;

    class FragmentBuffer
    {
        typedef std::pair<FragmentCPtr, size_t> frag_pair;
        typedef std::vector<frag_pair> frag_vec;
        
        frag_vec frags;
        size_t totalSz;

    public:
        void addFragment(FragmentCPtr const & p, size_t sz)
        {
            frags.push_back(std::make_pair(p, sz));
            totalSz += sz;
        }

        void getFragments(std::vector<Fragment const *> & out) const
        {
            for(frag_vec::const_iterator i = frags.begin();
                i != frags.end(); ++i)
            {
                out.push_back(i->first.get());
            }
        }
    };

private:
    inline Tablet * findTablet(strref_t row) const;

private:
    tablet_vec tablets;
    CommitRing rowCommits;
    FragmentBuffer memFrags;
};

#endif // KDI_SERVER_TABLE_H
