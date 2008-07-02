//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/hash_test.cc#1 $
//
// Created 2006/08/04
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

// Define this to enable Index-64 and Index-32 hashes.  Need to link
// against libcosmix.  For experimentation only.  Warp should *not* be
// tied to libcosmix in the checked-in source tree.
#define HAVE_LIBCOSMIX 0


#include "hsieh_hash.h"
#include "md5.h"
#include "file.h"
#include "util.h"
#include "strhash.h"
#include "strutil.h"
#include "timer.h"
#include "options.h"
#include <boost/format.hpp>
#include <iostream>
#include <algorithm>
#include <vector>
#include <map>

using namespace warp;
using namespace std;
using namespace boost;

namespace
{
//----------------------------------------------------------------------------
// 128 bit hash type
//----------------------------------------------------------------------------
    struct Hash128
    {
        uint64_t h[2];

        inline bool operator<(Hash128 const & o) const {
            return order(h[0], o.h[0], h[1], o.h[1]);
        }
        
        inline bool operator==(Hash128 const & o) const {
            return h[0] == o.h[0] && h[1] == o.h[1];
        }

        operator char() const { return char(h[0]); }
    };

    ostream & operator<<(ostream & o, Hash128 const & h) {
        return o << h.h[0] << h.h[1];
    }

//----------------------------------------------------------------------------
// Hash functors
//----------------------------------------------------------------------------
    struct Md5Hasher32
    {
        typedef uint32_t hash_t;
        enum { BITS = 32 };

        inline uint32_t operator()(void const * data, size_t len) const
        {
            uint32_t d[4];
            Md5::digest(&d, data, len);
            return d[0] ^ d[1] ^ d[2] ^ d[3];
        }

        char const * name() const { return "MD5-32"; }
    };

    struct Md5Hasher64
    {
        typedef uint64_t hash_t;
        enum { BITS = 64 };

        inline uint64_t operator()(void const * data, size_t len) const
        {
            uint64_t d[2];
            Md5::digest(&d, data, len);
            return d[0] ^ d[1];
        }

        char const * name() const { return "MD5-64"; }
    };

    struct Md5Hasher128
    {
        typedef Hash128 hash_t;
        enum { BITS = 128 };

        inline Hash128 operator()(void const * data, size_t len) const
        {
            Hash128 d;
            Md5::digest(&d, data, len);
            return d;
        }

        char const * name() const { return "MD5-128"; }
    };

    struct SimpleHasher32
    {
        typedef uint32_t hash_t;
        enum { BITS = 32 };

        inline uint32_t operator()(void const * data, size_t len) const
        {
            char const * p = reinterpret_cast<char const *>(data);
            return simpleHash<uint32_t>(p, p + len);
        }

        char const * name() const { return "Simple-32"; }
    };

    struct SimpleHasher64
    {
        typedef uint64_t hash_t;
        enum { BITS = 64 };

        inline uint64_t operator()(void const * data, size_t len) const
        {
            char const * p = reinterpret_cast<char const *>(data);
            return simpleHash<uint64_t>(p, p + len);
        }

        char const * name() const { return "Simple-64"; }
    };

    struct HsiehHasher
    {
        typedef uint32_t hash_t;
        enum { BITS = 32 };

        inline uint32_t operator()(void const * data, size_t len) const
        {
            return hsieh_hash(data, len);
        }

        char const * name() const { return "Hsieh-32"; }
    };



//----------------------------------------------------------------------------
// reportFreqs
//----------------------------------------------------------------------------
    template <class T>
    void reportFreqs(vector<T> & hashes)
    {
        sort(hashes.begin(), hashes.end());

        map<size_t, size_t> freqTable;
        size_t freq = 0;
        T last = T();
        bool first = true;
        for(typename vector<T>::const_iterator i = hashes.begin();
            i != hashes.end(); ++i)
        {
            if(!first && *i == last)
                ++freq;
            else
            {
                first = false;
                if(freq)
                    ++freqTable[freq];

                freq = 1;
                last = *i;
            }
        }
        if(freq)
            ++freqTable[freq];

        size_t total = 0;
        for(map<size_t,size_t>::const_iterator i = freqTable.begin();
            i != freqTable.end(); ++i)
        {
            total += i->first * i->second;
            cout << format("  frequency=%d : %d items\n") % i->first % i->second;
        }

        if(total)
        {
            size_t collisions = total - freqTable[1];
            cout << format("Collisions: %d\n") % collisions;
            cout << format("Collision rate: %0.3g\n") %
                (double(collisions) / total);
        }
    }


//----------------------------------------------------------------------------
// zipTest
//----------------------------------------------------------------------------
    template <class Hasher>
    void zipTest(Hasher const & testHash, size_t count, size_t length)
    {
        typedef typename Hasher::hash_t hash_t;

        if(!length)
            return;

        count = 1;
        for(size_t i = 0; i < length; ++i)
            count *= 10;

        char fmt[32];
        snprintf(fmt, sizeof(fmt), "%%0%lulu", length);
        
        cout << format("%s hashing %d %d-digit ASCII integers from %s to %s\n")
            % testHash.name()
            % count
            % length
            % str(format(fmt) % 0)
            % str(format(fmt) % (count-1));

        vector<hash_t> hashes;
        hashes.reserve(count);
        char buf[64];
        for(size_t i = 0; i < count; ++i)
        {
            snprintf(buf, sizeof(buf), fmt, i);
            hashes.push_back(testHash(buf, length));
        }

        reportFreqs(hashes);
    }

//----------------------------------------------------------------------------
// interactiveTest
//----------------------------------------------------------------------------
    template <class Hasher>
    void interactiveTest(Hasher const & testHash, vector<string> const & args)
    {
        typedef typename Hasher::hash_t hash_t;

        format fmt(str(format("  %%0%dx : %%s\n") % sizeof(hash_t)));
        cout << format("%s hashes:\n") % testHash.name();
        for(vector<string>::const_iterator ai = args.begin();
            ai != args.end(); ++ai)
        {
            hash_t h = testHash(ai->c_str(), ai->size());
            cout << fmt % h % reprString(wrap(*ai));
        }
    }

//----------------------------------------------------------------------------
// speedTest
//----------------------------------------------------------------------------
    template <class Hasher>
    void speedTest(Hasher const & testHash, size_t count, size_t length)
    {
        typedef typename Hasher::hash_t hash_t;

        if(!count)
            return;

        cout << format("Timing %s %s hash trials on data size %s\n")
            % testHash.name()
            % sizeString(count,1000)
            % sizeString(length);

        vector<char> buf;
        buf.reserve(length);
        for(size_t i = 0; i < length; ++i)
            buf.push_back(char((i+1) * 37));
        
        hash_t h;
        CpuTimer t;
        for(size_t i = 0; i < count; ++i)
        {
            h = testHash(&buf[0], length);
            buf[0] += (char)h;
        }

        double dt = t.getElapsed();

        cout << format("  cpu time: %.3f seconds  (%.3f usec avg.)\n")
            % dt % (dt * 1e6 / count);
    }


//----------------------------------------------------------------------------
// randomTest
//----------------------------------------------------------------------------
    template <class Hasher>
    void randomTest(Hasher const & testHash, size_t count, size_t length)
    {
        typedef typename Hasher::hash_t hash_t;

        cout << format("%s hashing %s random strings of length %s\n")
            % testHash.name() % sizeString(count,1000) % sizeString(length);

        vector<char> buf(length);
        vector<hash_t> hashes;
        hashes.reserve(count);
        for(size_t i = 0; i < count; ++i)
        {
            generate(buf.begin(), buf.end(), rand);
            hashes.push_back(testHash(&buf[0], length));
        }

        reportFreqs(hashes);
    }

//----------------------------------------------------------------------------
// dictionaryTest
//----------------------------------------------------------------------------
    template <class Hasher>
    void dictionaryTest(Hasher const & testHash, string const & fn)
    {
        typedef typename Hasher::hash_t hash_t;

        cout << format("%s hashing lines from dictionary: %s\n")
            % testHash.name() % fn;

        FilePtr fp = File::input(fn);

        vector<hash_t> hashes;
        vector<char> buf(4<<20);
        char * bufp = &buf[0];
        size_t buflen = buf.size();
        size_t len;
        while((len = fp->readline(bufp, buflen)))
        {
            if(len && bufp[len-1] == '\n')
                --len;
            hashes.push_back(testHash(bufp, len));
        }
        
        cout << format("Computed %d hashes:\n") % hashes.size();
        reportFreqs(hashes);
    }

//----------------------------------------------------------------------------
// dispatch
//----------------------------------------------------------------------------
    template <class Hasher>
    void dispatch(Hasher const & testHash, OptionMap const & opt,
                  ArgumentList const & args)
    {
        string arg;

        size_t count = 1;
        if(opt.get("count", arg))
            count = parseSize(arg);

        size_t length = 1;
        if(opt.get("length", arg))
            length = parseSize(arg);

        if(hasopt(opt, "zip"))
            zipTest(testHash, count, length);

        if(hasopt(opt, "speed"))
            speedTest(testHash, count, length);

        if(hasopt(opt, "random"))
            randomTest(testHash, count, length);

        if(opt.get("dictionary", arg))
            dictionaryTest(testHash, arg);
        
        if(!args.empty())
            interactiveTest(testHash, args);
    }
}



//----------------------------------------------------------------------------
// Index hash
//----------------------------------------------------------------------------
#if HAVE_LIBCOSMIX
#include "libcosmix/hash.h"
namespace
 {
    struct IndexHasher32
    {
        typedef uint32_t hash_t;
        enum { BITS = 32 };

        inline uint32_t operator()(void const * data, size_t len) const
        {
            int64_t h = hashFast(reinterpret_cast<char const *>(data), 0, len);
            return uint32_t(h ^ (h >> 32));
        }

        char const * name() const { return "Index-32"; }
    };

    struct IndexHasher64
    {
        typedef uint64_t hash_t;
        enum { BITS = 64 };

        inline uint64_t operator()(void const * data, size_t len) const
        {
            return uint64_t(hashFast(reinterpret_cast<char const *>(data), 0, len));
        }

        char const * name() const { return "Index-64"; }
    };
}
#endif


//----------------------------------------------------------------------------
// main
//----------------------------------------------------------------------------
int main(int ac, char ** av)
{
    using boost::program_options::value;
    OptionParser op;
    op.addOption("function,f", value<string>(), "Hash function to test");
    op.addOption("count,n", value<string>(), "Count for test (test-dependent interpretation)");
    op.addOption("length,l", value<string>(), "Length for test (test-dependent interpretation)");
    op.addOption("zip", "Do zipcode collision test");
    op.addOption("speed", "Do hash speed test");
    op.addOption("random", "Do random string test");
    op.addOption("dictionary", value<string>(), "Do dictionary string test");

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    string func;
    if(!opt.get("function", func))
        op.error("need --function");

    if(func == "md5-32")
        dispatch(Md5Hasher32(), opt, args);
    else if(func == "md5-64")
        dispatch(Md5Hasher64(), opt, args);
    else if(func == "md5-128")
        dispatch(Md5Hasher128(), opt, args);
    else if(func == "hsieh-32")
        dispatch(HsiehHasher(), opt, args);
    else if(func == "simple-32")
        dispatch(SimpleHasher32(), opt, args);
    else if(func == "simple-64")
        dispatch(SimpleHasher64(), opt, args);
#if HAVE_LIBCOSMIX
    else if(func == "index-32")
        dispatch(IndexHasher32(), opt, args);
    else if(func == "index-64")
        dispatch(IndexHasher64(), opt, args);
#endif
    else
        op.error(string("unknown --function: ") + func);

    return 0;
}
