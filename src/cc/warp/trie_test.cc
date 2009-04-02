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

    trie.nNodes = 0;
    trie.nCharCmp = 0;
    trie.nNodeCmp = 0;
    size_t nChars = 0;

    
    {
        PageCounts c;
        if(readPageCounts(&c))
            cout << format("Proc data size: %sB")
                % sizeString(c.data * getPageSize())
                 << endl;
    }


    CpuTimer cpuTimer;
    WallTimer wallTimer;
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

            loadSz += end - begin;
            nChars += end - begin;

#if 1
            trie.insert(StringRange(begin, end), lineno);

#elif 0
            hmap[std::string(begin, end)] = lineno;

#elif 0
            tmap[std::string(begin, end)] = lineno;

#elif 0
            wmap.set(std::string(begin, end), lineno);

#endif

#if 1
            size_t const K = 50000;
            if((lineno % K) == 0)
            {
                cout << format("line %d, %sB inserted, %s nodes, %s nodeCmp, %s charCmp, %.3f ncmp/line, %.3f ccmp/line, %.3f chars/line")
                    % lineno
                    % sizeString(nChars, 1000)
                    % sizeString(trie.nNodes, 1000)
                    % sizeString(trie.nNodeCmp, 1000)
                    % sizeString(trie.nCharCmp, 1000)
                    % (double(trie.nNodeCmp) / K)
                    % (double(trie.nCharCmp) / K)
                    % (double(nChars) / K)
                     << endl;

                trie.nNodes = 0;
                trie.nCharCmp = 0;
                trie.nNodeCmp = 0;
                nChars = 0;
            }
#endif
        }

        nLoaded += lineno;
    }

    double wdt = wallTimer.getElapsed();
    double cdt = cpuTimer.getElapsed();
    cout << format("Load time: %.3f seconds, %.3f CPU seconds")
        % wdt % cdt << endl;

    cout << format("Loaded %d items: %.1f ns/item")
        % nLoaded % (cdt * 1e9 / nLoaded) << endl;

    cout << format("Loaded %s bytes: %.1f ns/bytes")
        % sizeString(loadSz) % (cdt * 1e9 / loadSz) << endl;

    {
        PageCounts c;
        if(readPageCounts(&c))
            cout << format("Proc data size: %sB")
                % sizeString(c.data * getPageSize())
                 << endl;
    }

    //trie.dump(cout);

    return 0;
}
