//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-17
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

#include <kdi/server/Tablet.h>
#include <kdi/server/TableSchema.h>
#include <kdi/server/TabletConfig.h>
#include <warp/Callback.h>
#include <ex/exception.h>
#include <memory>

using namespace kdi;
using namespace kdi::server;

namespace {

    class CbCaller
        : public warp::Runnable
    {
    public:
        void run()
        {
            for(std::vector<warp::Callback *>::const_iterator i = cbs.begin();
                i != cbs.end(); ++i)
            {
                (*i)->done();
            }
            delete this;
        }

        void addCallback(warp::Callback * cb)
        {
            cbs.push_back(cb);
        }

    private:
        std::vector<warp::Callback *> cbs;
    };

}

//----------------------------------------------------------------------------
// Tablet
//----------------------------------------------------------------------------
Tablet::Tablet(warp::IntervalPoint<std::string> const & lastRow) :
    lastRow(lastRow),
    state(TABLET_CONFIG_LOADING)
{
}

Tablet::~Tablet()
{
}

void Tablet::applySchema(TableSchema const & schema)
{
    // xxx lazy...
    using namespace ex;
    if(schema.groups.size() != 1)
        raise<RuntimeError>("only support single locality group");

    fragGroups.resize(schema.groups.size());
}

void Tablet::getConfigFragments(TabletConfig & cfg) const
{
    size_t n = 0;
    for(fragvec_vec::const_iterator i = fragGroups.begin();
        i != fragGroups.end(); ++i)
    {
        n += i->size();
    }

    if(loadingConfig)
        n += loadingConfig->fragments.size();

    cfg.fragments.clear();
    cfg.fragments.resize(n);

    std::vector<TabletConfig::Fragment>::iterator oi = cfg.fragments.begin();

    if(loadingConfig)
    {
        oi = std::copy(loadingConfig->fragments.begin(),
                       loadingConfig->fragments.end(),
                       oi);
    }

    for(fragvec_vec::const_iterator i = fragGroups.begin();
        i != fragGroups.end(); ++i)
    {
        for(frag_vec::const_iterator j = i->begin();
            j != i->end(); ++j, ++oi)
        {
            oi->filename = (*j)->getFilename();
            (*j)->getColumnFamilies(oi->families);
        }
    }
}

void Tablet::onConfigLoaded(TabletConfigCPtr const & config)
{
    assert(state == TABLET_CONFIG_LOADING);
    assert(config->rows.getUpperBound() == lastRow);
    
    firstRow = config->rows.getLowerBound();
    if(!config->fragments.empty())
        loadingConfig = config;
}

void Tablet::onFragmentsLoaded(
    TableSchema const & schema,
    std::vector<FragmentCPtr> const & fragments)
{
    assert(state == TABLET_FRAGMENTS_LOADING);
    assert(loadingConfig);
    assert(schema.groups.size() == fragGroups.size());

    // xxx lazy...
    using namespace ex;
    if(schema.groups.size() != 1)
        raise<RuntimeError>("only support single locality group");

    fragGroups[0].insert(
        fragGroups[0].begin(),
        fragments.begin(),
        fragments.end());

    loadingConfig.reset();
}

void Tablet::deferUntilState(warp::Callback * cb, TabletState state)
{
    cbHeap.push(state_cb(state, cb));
}

warp::Runnable * Tablet::setState(TabletState state)
{
    std::auto_ptr<CbCaller> r;

    this->state = state;

    while(!cbHeap.empty())
    {
        if(cbHeap.top().first > state)
            break;

        if(!r.get())
            r.reset(new CbCaller);

        r->addCallback(cbHeap.top().second);
        cbHeap.pop();
    }

    return r.release();
}
