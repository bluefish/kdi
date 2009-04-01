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
#include <iostream>
#include <boost/format.hpp>

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

    trie.nNodes = 0;
    trie.nCharCmp = 0;
    trie.nNodeCmp = 0;
    size_t nChars = 0;

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

            nChars += end - begin;

            trie.insert(StringRange(begin, end), lineno);

            if((lineno % 1000) == 0)
            {
                cout << format("line %d, %sB inserted, %s nodes, %s nodeCmp, %s charCmp, %.3f ncmp/line, %.3f ccmp/byte")
                    % lineno
                    % sizeString(nChars, 1000)
                    % sizeString(trie.nNodes, 1000)
                    % sizeString(trie.nNodeCmp, 1000)
                    % sizeString(trie.nCharCmp, 1000)
                    % (double(trie.nNodeCmp) / 1000)
                    % (double(trie.nCharCmp) / nChars)
                     << endl;

                trie.nNodes = 0;
                trie.nCharCmp = 0;
                trie.nNodeCmp = 0;
                nChars = 0;
            }
        }
    }

    return 0;
}
