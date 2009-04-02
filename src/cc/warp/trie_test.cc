//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-31
//
// This file is part of the warp library.
//
// The warp library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
//
// The warp library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#include <warp/StringTrie.h>
#include <warp/file.h>
#include <warp/buffer.h>
#include <warp/options.h>
#include <warp/strutil.h>
#include <warp/sysinfo.h>
#include <warp/timer.h>
#include <iostream>
#include <boost/format.hpp>

#include <tr1/unordered_map>
#include <map>
#include <string>
#include <warp/hashmap.h>
#include <warp/strhash.h>

using namespace warp;
using namespace std;
using boost::format;

int main(int ac, char ** av)
{
    OptionParser op;
    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    StringTrie<size_t> trie;
    std::tr1::unordered_map<std::string, size_t> hmap;
    std::map<std::string, size_t> tmap;
    warp::HashMap<std::string, size_t, HsiehHash> wmap;

    {
        PageCounts c;
        if(readPageCounts(&c))
            cout << format("Proc data size: %sB")
                % sizeString(c.data * getPageSize())
                 << endl;
    }

    cout << "Loading lines" << endl;
    typedef std::vector<std::pair<string, size_t> > vec_t;
    vec_t lines;
    
    size_t nLoaded = 0;
    size_t loadSz = 0;
    for(size_t i = 0; i < args.size(); ++i)
    {
        FilePtr f = File::input(args[i]);
        size_t lineno = 0;

        for(Buffer buf(128 << 10);
            f->readline(buf, 128 << 20);
            buf.clear())
        {
            ++lineno;

            buf.flip();
            char const * begin = buf.position();
            char const * end = buf.limit();

            begin = skipSpace(begin, end);
            while(end != begin && (end[-1] == '\n' || end[-1] == '\r'))
                --end;
            end = skipTrailingSpace(begin, end);

            lines.push_back(std::make_pair(std::string(begin,end), lineno));

            loadSz += end - begin;
        }

        nLoaded += lineno;
    }

    cout << "Loaded lines" << endl;

    size_t baseSz = 0;
    PageCounts c;
    {
        if(readPageCounts(&c))
        {
            baseSz = c.data * getPageSize();
            cout << format("Proc data size: %sB")
                % sizeString(baseSz)
                 << endl;
        }
    }

    cout << "Loading map" << endl;

    WallTimer wallTimer;
    CpuTimer cpuTimer;

    for(vec_t::const_iterator i = lines.begin(); i != lines.end(); ++i)
    {
        StringRange key(i->first);
        size_t val = i->second;

#if 1
        trie.insert(key, val);
#elif 0
        hmap[key.toString()] = val;
#elif 0
        tmap[key.toString()] = val;
#elif 0
        wmap.set(key.toString(), val);
#endif
    }

    double cdt = cpuTimer.getElapsed();
    double wdt = wallTimer.getElapsed();
    cout << format("Load time: %.3f seconds, %.3f CPU seconds")
        % wdt % cdt << endl;

    cout << format("Loaded %d items: %.1f ns/item")
        % nLoaded % (cdt * 1e9 / nLoaded) << endl;

    cout << format("Loaded %s bytes: %.1f ns/bytes")
        % sizeString(loadSz) % (cdt * 1e9 / loadSz) << endl;

    size_t mapSz = 0;
    {
        PageCounts c;
        if(readPageCounts(&c))
        {
            size_t curSz = c.data * getPageSize();
            cout << format("Proc data size: %sB")
                % sizeString(curSz)
                 << endl;
            mapSz = curSz - baseSz;
        }
    }

    cout << format("Map size: %sB, %.2f bytes/item, avg item = %.2f bytes, %.2f bytes/item overhead")
        % sizeString(mapSz)
        % (double(mapSz) / nLoaded)
        % (double(loadSz) / nLoaded)
        % (double(mapSz - loadSz) / nLoaded)
         << endl;

    //trie.dump(cout);

    return 0;
}
