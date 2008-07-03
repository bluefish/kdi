//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-10
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#include <warp/options.h>
#include <warp/strutil.h>
#include <warp/fs.h>
#include <stdlib.h>

#include <string>
#include <list>
#include <vector>

using namespace warp;
using namespace std;

namespace po = boost::program_options;

//----------------------------------------------------------------------------
// OptionMap
//----------------------------------------------------------------------------
OptionMap::OptionMap(super const & s, std::set<std::string> const & tweaks)
    : super(s), tweaks(tweaks)
{
}


//----------------------------------------------------------------------------
// OptionParser
//----------------------------------------------------------------------------
OptionParser::OptionParser(std::string const & usage,
                           std::string const & optCaption,
                           bool autoHelp) :
    optionDesc(optCaption),
    progName("<command>"),
    usageFmt(usage)
{
    if(autoHelp)
        optionDesc.add_options()("help,h", "Show help message");
}

void OptionParser::addOption(char const * name, char const * help)
{
    optionDesc.add_options()(name, help);
}

void OptionParser::addOption(char const * name, ValueSemantic const * val,
                             char const * help)
{
    optionDesc.add_options()(name, val, help);
}

void OptionParser::addSection(OptionParser const & op)
{
    optionDesc.add(op.optionDesc);
}

void OptionParser::parse(int ac, char ** av, Options & opt,
                         Arguments & args)
{
    typedef std::vector<std::string> ArgList;

    if(ac > 0)
        progName = fs::basename(av[0]);

    try
    {
        set<string> munged;
        vector<char *> avhack(ac+1);
        for(int i = 0; i < ac; ++i)
        {
            char * arg = av[i];
            size_t len = strlen(arg);

            // Hack to fix Boost.Program_Options quote stripping
            if(len >= 2 && arg[0] == arg[len-1]
               && (arg[0] == '\'' || arg[0] == '"'))
            {
                // Append space.  That seems to fool the parser
                avhack[i] = const_cast<char *>(
                    munged.insert(
                        string(arg, arg+len) + " "
                        ).first->c_str());
            }
            else
            {
                avhack[i] = arg;
            }
        }
        avhack[ac] = 0;

        po::options_description desc;
        desc.add(optionDesc);
        desc.add_options()("positional_args",
                           po::value<ArgList>()->composing(),
                           "");

        po::positional_options_description pdesc;
        pdesc.add("positional_args", -1);

        namespace pos = po::command_line_style;
        int style = pos::unix_style & (~pos::allow_guessing);

        po::command_line_parser cmd_parser(ac, &avhack[0]);
        cmd_parser.options(desc);
        cmd_parser.positional(pdesc);
        cmd_parser.style(style);

        po::variables_map vm;
        po::store(cmd_parser.run(), vm);
        po::notify(vm);

        opt = OptionMap(vm, munged);
        if(vm.count("positional_args"))
            args = vm["positional_args"].as<ArgList>();
        else
            args.clear();

        // Un-munge positional arguments
        for(ArgumentList::iterator i = args.begin(); i != args.end(); ++i)
        {
            // Unmunge: remove the trailing space
            if(munged.find(*i) != munged.end())
                i->erase(i->size()-1);
        }
    }
    catch(po::error const & ex) {
        ex::raise<OptionError>("%s", ex.what());
    }

    if(opt.count("help"))
    {
        using std::endl;
        using std::cout;
        cout << "usage: " << getUsage() << endl
             << endl
             << optionDesc;
        exit(0);
    }
}

void OptionParser::parseOrBail(int ac, char ** av, Options & opt,
                               Arguments & args)
{
    try {
        parse(ac, av, opt, args);
    }
    catch(OptionError const & ex) {
        error(ex.what());
    }
}
    
std::string OptionParser::getUsage() const
{
    return replace(usageFmt, "%prog", progName);
}

void OptionParser::setUsage(std::string const & usage)
{
    usageFmt = usage;
}

void OptionParser::error(std::string const & message, bool abortProgram) const
{
    using std::endl;
    using std::cerr;
    cerr << "usage: " << getUsage() << endl
         << endl
         << progName << ": error: " << message << endl;

    if(abortProgram)
        exit(2);
}
