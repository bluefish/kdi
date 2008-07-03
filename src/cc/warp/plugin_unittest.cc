//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-04-23
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

#include <warp/plugin.h>
#include <unittest/main.h>
#include <warp/file.h>
#include <warp/tmpfile.h>
#include <warp/fs.h>
#include <ex/exception.h>
#include <boost/format.hpp>
#include <stdlib.h>
#include <string>
#include <iostream>
#include <boost/function.hpp>

using namespace warp;
using namespace ex;
using namespace std;

namespace
{
    class LibMaker
    {
        string srcFn;
        string libFn;

    public:
        explicit LibMaker(char const * prog)
        {
            FilePtr fp = openTmpFile("/tmp", "libplugin_unittest-");
            srcFn = fp->getName();
            fp->write(prog, strlen(prog));
            fp->close();

            libFn = srcFn + ".so";

            string cmd = (boost::format("g++ -rdynamic -shared -fPIC -x c++ -o %s %s")
                          % libFn % srcFn).str();

            cerr << "Compiling: " << cmd << endl;
            int status = system(cmd.c_str());

            cerr << "  done, status=" << status << endl;
            if(status != 0)
                raise<RuntimeError>("cmd failed (status=%d): %s", status, cmd);
        }

        ~LibMaker()
        {
            if(!srcFn.empty())
            {
                try {
                    cerr << "Removing " << srcFn << endl;
                    fs::remove(srcFn);
                }
                catch(...) {}
            }
            if(!libFn.empty())
            {
                try { 
                    cerr << "Removing " << libFn << endl;
                    fs::remove(libFn);
                }
                catch(...) {}
            }
        }

        string const & getName() const { return libFn; }
    };
}

BOOST_AUTO_UNIT_TEST(smoke_test)
{
    // Basic smoke test

    LibMaker lib(
        "extern \"C\" {"
        "int add(int a, int b)"
        "{"
        "    return a + b;"
        "}"
        "}\n");

    // Load plugin
    Plugin plugin(lib.getName());

    // Try to load a plugin that doesn't exist
    BOOST_CHECK_THROW(Plugin("/does/not/exist/i/hope.so"), RuntimeError);
    
    // Check findFunction()
    BOOST_CHECK(plugin.findFunction<int (int,int)>("add") != 0);
    BOOST_CHECK(plugin.findFunction<int (int,int)>("subtract") == 0);

    // Check getFunction()
    BOOST_CHECK(plugin.getFunction<int (int,int)>("add") != 0);
    BOOST_CHECK_THROW(plugin.getFunction<int (int,int)>("subtract"), RuntimeError);

    // Check getFunctor()
    PluginFunctor<int (int,int)> add1 = plugin.getFunctor<int (int,int)>("add");
    BOOST_CHECK_EQUAL(add1(3,4), 7);
    BOOST_CHECK_THROW(plugin.getFunctor<int (int,int)>("subtract"), RuntimeError);

    // Check PluginFunctor(Plugin,string) constructor
    PluginFunctor<int (int,int)> add2(plugin, "add");
    BOOST_CHECK_EQUAL(add1(5,10), 15);
    BOOST_CHECK_THROW(PluginFunctor<int (int,int)>(plugin, "subtract"), RuntimeError);

    // Check PluginFunctor(string,string) constructor
    PluginFunctor<int (int,int)> add3(lib.getName(), "add");
    BOOST_CHECK_EQUAL(add3(4,8), 12);
    BOOST_CHECK_THROW(PluginFunctor<int (int,int)>(lib.getName(), "subtract"), RuntimeError);
    BOOST_CHECK_THROW(PluginFunctor<int (int,int)>("/DNE/nope/nothing.so", "add"), RuntimeError);
    
#if 0
    // These should produce compiler errors (and they do)
    add1();
    add1(4);
    add1(3,"hello");
    add1(1,2,3);
    char const * x = add1(1,2);
#endif
}

BOOST_AUTO_UNIT_TEST(boost_function_compat)
{
    // Make plugin functors are compatible with boost::function

    LibMaker lib(
        "extern \"C\" {"
        "int add(int a, int b)"
        "{"
        "    return a + b;"
        "}"
        "int square(int x)"
        "{"
        "    return x * x;"
        "}"
        "}\n");


    boost::function<int (int,int)> add;
    boost::function<int (int)> square;

    {
        Plugin plugin(lib.getName());

        // Make sure assignment works
        add = plugin.getFunction<int (int,int)>("add");
        square = PluginFunctor<int (int)>(lib.getName(), "square");

        // Call the functions
        BOOST_CHECK_EQUAL(add(1,2), 3);
        BOOST_CHECK_EQUAL(square(-8), 64);
    }

    // Plugin library has gone out of scope.  Functions should still
    // be valid.
    BOOST_CHECK_EQUAL(add(7,7), 14);
    BOOST_CHECK_EQUAL(square(6), 36);
}
