//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-30
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

#include <kdi/net/StatReporter.h>
#include <warp/options.h>
#include <Ice/Ice.h>
#include <algorithm>
#include <sstream>
#include <iostream>
#include <string>

using kdi::net::details::StatReporterPrx;
using kdi::net::details::StatMap;
using namespace warp;
using namespace std;

namespace {

    Ice::CommunicatorPtr const & getCommunicator()
    {
        int ac = 2;
        char const * av[] = { "foo", "--Ice.MessageSizeMax=32768" };

        static Ice::CommunicatorPtr com(
            Ice::initialize(ac, const_cast<char **>(av)));
        return com;
    }

    StatReporterPrx getReporter(std::string const & hostPort)
    {
        char const * begin = hostPort.c_str();
        char const * end = begin + hostPort.size();
        char const * hostEnd = std::find(begin, end, ':');
        char const * portBegin = hostEnd;
        if(portBegin != end)
            ++portBegin;

        ostringstream oss;
        oss << "StatReporter:tcp -h ";
        if(begin != hostEnd)
            oss.write(begin, hostEnd - begin);
        else
            oss << "localhost";
        oss << " -p ";
        if(portBegin != end)
            oss.write(portBegin, end - portBegin);
        else
            oss << "34177";

        Ice::CommunicatorPtr ic = getCommunicator();
        return StatReporterPrx::checkedCast(ic->stringToProxy(oss.str()));
    }

}

int main(int ac, char ** av)
{
    OptionParser op("%prog [options] [HOST[:PORT]] ...");
    {
        using namespace boost::program_options;
    }

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    if(args.empty())
        args.push_back("");

    for(ArgumentList::const_iterator i = args.begin();
        i != args.end(); ++i)
    {
        StatReporterPrx reporter = getReporter(*i);
        
        StatMap stats;
        reporter->getStats(stats);

        for(StatMap::const_iterator i = stats.begin();
            i != stats.end(); ++i)
        {
            cout << i->first << ' ' << i->second << endl;
        }
    }

    return 0;
}
