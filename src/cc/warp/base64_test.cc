//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-08-28
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

#include <warp/base64.h>
#include <warp/options.h>
#include <warp/file.h>
#include <string>

using namespace warp;
using namespace std;

namespace
{
    char const * const ASCII_STABLE = 
        "+/0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    char const * const STANDARD_ALPHABET = 
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789+/";
}

int main(int ac, char ** av)
{
    OptionParser op("%prog [options] files...");
    {
        using namespace boost::program_options;
        op.addOption("alphabet,a", value<string>(), "use custom alphabet");
        op.addOption("stable,s", "use alphabet with stable ordering");
        op.addOption("decode,d", "decode instead of encode");
        op.addOption("output,o", value<string>()->default_value("-"),
                     "output file");
    }

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    char const * alphabet = STANDARD_ALPHABET;
    string alphabetStr;
    if(opt.get("alphabet", alphabetStr))
    {
        if(hasopt(opt, "stable"))
            op.error("--stable and --alphabet are exclusive");
        alphabet = alphabetStr.c_str();
    }
    else if(hasopt(opt, "stable"))
        alphabet = ASCII_STABLE;

    string outputStr;
    opt.get("output", outputStr);
    FilePtr output = File::output(outputStr);
    
    if(hasopt(opt, "decode"))
    {
        Base64Decoder decoder(alphabet);
        char inBuf[4000];
        char outBuf[3000];

        for(ArgumentList::const_iterator i = args.begin();
            i != args.end(); ++i)
        {
            FilePtr input = File::input(*i);
            size_t sz;
            while((sz = input->read(inBuf, sizeof(inBuf))))
            {
                char * end = decoder.decode_iter(
                    inBuf, inBuf + sz,
                    &outBuf[0]);
                output->write(outBuf, end - outBuf);
            }
        }
    }
    else
    {
        Base64Encoder encoder(alphabet);
        char inBuf[3000];
        char outBuf[4000];

        for(ArgumentList::const_iterator i = args.begin();
            i != args.end(); ++i)
        {
            FilePtr input = File::input(*i);
            size_t sz;
            while((sz = input->read(inBuf, sizeof(inBuf))))
            {
                char * end = encoder.encode_iter(
                    StringRange(inBuf, sz),
                    &outBuf[0]);
                output->write(outBuf, end - outBuf);
            }
        }
    }

    output->close();

    return 0;
}
