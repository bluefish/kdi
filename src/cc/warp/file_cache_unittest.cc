//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-06-12
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

#include <warp/file_cache.h>
#include <warp/fs.h>
#include <unittest/main.h>
#include <unittest/predicates.h>
#include <iostream>

using namespace warp;
using namespace std;

namespace
{
    void genRandomFile(std::string const & name, size_t sz)
    {
        cout << "generating " << name << endl;

        FilePtr out = File::output(name);
        size_t written = 0;
        while(written < sz)
        {
            int32_t x = rand();
            size_t n = out->write(&x, std::min(sz-written, sizeof(x)));
            if(!n)
                break;
            written += n;
        }

        cout << "  wrote " << written << " bytes" << endl;
    }
    
    using unittest::predicate_result;

    predicate_result equalSubrange(FilePtr const & f1, FilePtr const & f2,
                                   size_t begin, size_t len)
    {
        cout << "checking " << f1->getName() << " vs. " << f2->getName() << endl;

        predicate_result r(true);

        cout << "  seeking to " << begin << endl;
        f1->seek(begin);
        f2->seek(begin);
        
        size_t const BUF_SZ = 4 << 10;
        char buf1[BUF_SZ];
        char buf2[BUF_SZ];
        
        while(len)
        {
            size_t readSz = std::min(len, BUF_SZ);
            cout << "  reading " << begin << " to "
                 << (begin + readSz) << endl;

            size_t s1 = f1->read(buf1, readSz);
            size_t s2 = f2->read(buf2, readSz);

            if(s1 != s2)
            {
                r = false;
                r.message() << "different read lengths at position " << begin
                            << ": f1=" << f1->getName() << " s1=" << s1
                            <<  " f2=" << f2->getName() << " s2=" << s2;
                return r;
            }

            if(!s1)
            {
                cout << "  EOF" << endl;
                break;
            }
            
            if(!std::equal(buf1, buf1+s1, buf2))
            {
                r = false;
                r.message() << "different contents between " << begin
                            << " and " << (begin+s1)
                            << ": f1=" << f1->getName() << " s1=" << s1
                            <<  " f2=" << f2->getName() << " s2=" << s2;
                return r;
            }

            len -= s1;
            begin += s1;
        }

        return r;
    }
}

BOOST_AUTO_UNIT_TEST(cache_basic)
{
    FileCachePtr cache = FileCache::make(1<<20, 10);

    genRandomFile("memfs:f1", 1234567);
    genRandomFile("memfs:f2", 0);
    genRandomFile("memfs:f3", 321);
    genRandomFile("memfs:f4", 1);
    genRandomFile("memfs:f5", 2222222);

    {
        FilePtr f1 = File::input("memfs:f1");
        FilePtr f2 = cache->open("memfs:f1");

        BOOST_CHECK(equalSubrange(f1, f2, 0, 1));
        BOOST_CHECK(equalSubrange(f1, f2, 0, 10000000));
        BOOST_CHECK(equalSubrange(f1, f2, 1000000, 1000000));
        BOOST_CHECK(equalSubrange(f1, f2, 43212, 63142));
    }

    {
        FilePtr f1 = File::input("memfs:f2");
        FilePtr f2 = cache->open("memfs:f2");

        BOOST_CHECK(equalSubrange(f1, f2, 0, 1));
        BOOST_CHECK(equalSubrange(f1, f2, 0, 10000000));
        BOOST_CHECK(equalSubrange(f1, f2, 1000000, 1000000));
        BOOST_CHECK(equalSubrange(f1, f2, 43212, 63142));
    }

    {
        FilePtr f1 = File::input("memfs:f3");
        FilePtr f2 = cache->open("memfs:f3");

        BOOST_CHECK(equalSubrange(f1, f2, 0, 1));
        BOOST_CHECK(equalSubrange(f1, f2, 0, 10000000));
        BOOST_CHECK(equalSubrange(f1, f2, 1000000, 1000000));
        BOOST_CHECK(equalSubrange(f1, f2, 43212, 63142));
    }
    
    {
        FilePtr f1 = File::input("memfs:f4");
        FilePtr f2 = cache->open("memfs:f4");

        BOOST_CHECK(equalSubrange(f1, f2, 0, 1));
        BOOST_CHECK(equalSubrange(f1, f2, 0, 10000000));
        BOOST_CHECK(equalSubrange(f1, f2, 1000000, 1000000));
        BOOST_CHECK(equalSubrange(f1, f2, 43212, 63142));
    }

    {
        FilePtr f1 = File::input("memfs:f5");
        FilePtr f2 = cache->open("memfs:f5");

        BOOST_CHECK(equalSubrange(f1, f2, 0, 1));
        BOOST_CHECK(equalSubrange(f1, f2, 0, 10000000));
        BOOST_CHECK(equalSubrange(f1, f2, 1000000, 1000000));
        BOOST_CHECK(equalSubrange(f1, f2, 43212, 63142));
    }

    fs::remove("memfs:f1");
    fs::remove("memfs:f2");
    fs::remove("memfs:f3");
    fs::remove("memfs:f4");
    fs::remove("memfs:f5");
    fs::remove("memfs:f6");
}

