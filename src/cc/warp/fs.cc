//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-09-19
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

#include <warp/fs.h>
#include <warp/dir.h>
#include <warp/uri.h>
#include <warp/vstring.h>
#include <warp/strutil.h>
#include <warp/strhash.h>
#include <warp/hashmap.h>
#include <warp/timestamp.h>
#include <ex/exception.h>
#include <fnmatch.h>

#include <iostream>
#include <boost/format.hpp>
using boost::format;

using namespace warp;
using namespace ex;
using namespace std;

//----------------------------------------------------------------------------
// Internals
//----------------------------------------------------------------------------
namespace
{
    // File system registration table type
    typedef HashMap<string, FsMapFunc *, HsiehHash> table_t;

    struct Details
    {
        table_t regTable;
        FsMapFunc * defaultFunc;
        
        static Details & get()
        {
            static Details d;
            return d;
        }

    private:
        Details() : defaultFunc(0) {}
    };


    // Get filesystem registration table
    table_t & getTable()
    {
        return Details::get().regTable;
    }

    // Get default filesystem mapping function (may be null)
    FsMapFunc * & defaultFunc()
    {
        return Details::get().defaultFunc;
    }
}


//----------------------------------------------------------------------------
// Dispatch mechanism
//----------------------------------------------------------------------------
FsPtr Filesystem::get(string const & uriStr)
{
    Uri uri(wrap(uriStr));
    FsMapFunc * func = 0;

    if(uri.schemeDefined())
    {
        func = getTable().get(uri.scheme, 0);
        if(!func)
            func = getTable().get(uri.topScheme(), 0);
        if(!func)
            raise<RuntimeError>("unrecognized filesystem scheme '%s': %s",
                                uri.scheme, uri);
    }
    else
    {
        func = defaultFunc();
        if(!func)
            raise<RuntimeError>("no default filesystem scheme: %s", uri);
    }

    return (*func)(uriStr);
}

void Filesystem::registerScheme(char const * scheme, FsMapFunc * func)
{
    if(!scheme || !*scheme)
        raise<ValueError>("null scheme name");
    if(!func)
        raise<ValueError>("null handler for scheme '%1%'", scheme);

    getTable().set(scheme, func);
}

void Filesystem::setDefaultScheme(char const * scheme)
{
    if(!scheme)
        defaultFunc() = 0;
    else
    {
        FsMapFunc * func = getTable().get(scheme, 0);
        if(!func)
            raise<RuntimeError>("unrecognized filesystem scheme: %s", scheme);
        defaultFunc() = func;
    }
}


//----------------------------------------------------------------------------
// Free functions
//----------------------------------------------------------------------------
string fs::resolve(string const & baseUri, string const & relUri)
{
    Uri base(wrap(baseUri));
    VString altBase;
    if(base.path && base.path.end()[-1] != '/')
    {
        // Add a trailing slash to path component
        altBase.reserve(base.size() + 1);
        altBase.append(base.begin(), base.path.end());
        altBase += '/';
        altBase.append(base.path.end(), base.end());
        base = Uri(&altBase[0], &altBase[0] + altBase.size());
    }
    
    Uri ref(wrap(relUri));

    VString result;
    base.resolve(ref, result);
    return string(result.begin(), result.end());
}

string fs::path(string const & uri)
{
    Uri u(wrap(uri));
    return string(u.path.begin(), u.path.end());
}

string fs::replacePath(string const & origUri, string const & replacementPath)
{
    Uri u(wrap(origUri));
    string r(u.begin(), u.path.begin());
    if(u.authority &&
       (replacementPath.empty() || replacementPath[0] != '/'))
    {
        // Having an Authority implies that all Paths must be
        // absolute.  Force a slash.
        r += '/';
    }
    r.append(replacementPath);
    r.append(u.path.end(), u.end());
    return r;
}

string fs::basename(string const & uri, bool stripExt)
{
    Uri u(wrap(uri));

    char const * baseBegin = u.path.begin();
    char const * baseEnd = u.path.end();

    // Find last slash
    for(char const * p = baseEnd; p != baseBegin; --p)
    {
        if(p[-1] == '/')
        {
            baseBegin = p;
            break;
        }
    }

    // Find beginning of extension
    if(stripExt)
    {
        for(char const * p = baseEnd; p != baseBegin; --p)
        {
            if(p[-1] == '.')
            {
                if(--p != baseBegin && p[-1] != '/')
                    baseEnd = p;
                break;
            }
        }
    }
    
    return string(baseBegin, baseEnd);
}

string fs::dirname(string const & uri)
{
    Uri u(wrap(uri));
    char const * p;
    for(p = u.path.end(); p != u.path.begin(); --p)
    {
        if(p[-1] == '/')
        {
            --p;
            break;
        }
    }
    
    return string(u.path.begin(), p);
}

string fs::extension(string const & uri)
{
    Uri u(wrap(uri));
    for(char const * p = u.path.end(); p != u.path.begin(); --p)
    {
        if(p[-1] == '.')
        {
            if(--p != u.path.begin() && p[-1] != '/')
                return string(p, u.path.end());
            break;
        }
        else if(p[-1] == '/')
            break;
    }
    return string();
}

string fs::changeExtension(string const & uri, string const & newExt)
{
    Uri u(wrap(uri));

    // Find extension in path
    char const * oldExtBegin = u.path.end();
    for(char const * p = u.path.end(); p != u.path.begin(); --p)
    {
        // Look for extension character
        if(p[-1] == '.')
        {
            // Only use extension if it is not the entire basename
            if(--p != u.path.begin() && p[-1] != '/')
                oldExtBegin = p;
            break;
        }
        // Check for end of basename
        else if(p[-1] == '/')
        {
            // Don't change anything if the basename is empty
            if(p == u.path.end())
                return uri;
            break;
        }
    }

    // Replace old extension with new
    string r(u.begin(), oldExtBegin);
    r.append(newExt);
    r.append(u.path.end(), u.end());
    return r;
}

void fs::makedirs(string const & uri)
{
    if(path(uri).empty() || isDirectory(uri))
        return;

    string parent = resolve(uri, "..");
    if(parent != uri)
        makedirs(parent);

    mkdir(uri);
}

void fs::removedirs(std::string const & uri)
{
    if(isDirectory(uri))
    {
        DirPtr dir = Directory::open(uri);
        string p;
        while(dir->readPath(p))
            removedirs(p);
    }
    remove(uri);
}

bool fs::isRooted(std::string const & uri)
{
    Uri u(wrap(uri));
    return u.path && *u.path.begin() == '/';
}

bool fs::isBackpath(std::string const & uri)
{
    Uri u(wrap(uri));
    size_t len = u.path.size();
    char const * p = u.path.begin();
    
    if(len == 2)
    {
        return (p[0] == '.' &&
                p[1] == '.');
    }
    else if(len >= 3)
    {
        return (p[0] == '.' &&
                p[1] == '.' &&
                p[1] == '/');
    }
    else
    {
        return false;
    }
}


// globbing disabled (see header)
#if 0

vector<string> fs::glob(string const & uri)
{
    vector<string> result;
    globAppend(uri, result);
    return result;
}

void fs::globAppend(string const & uri, vector<string> & result)
{
    cerr << format("  glob: %s\n") % reprString(wrap(uri));

    Uri u(wrap(uri));

    // Find a wildcard (star)
    char const * star = std::find(u.path.begin(), u.path.end(), '*');
    if(star == u.path.end())
    {
        if(exists(uri))
            result.push_back(uri);
        return;
    }

    // Split path segments around wildcard
    char const * s0 = star;
    char const * s1 = star + 1;
    while(s0 != u.path.begin() && s0[-1] != '/')
        --s0;
    while(s1 != u.path.end() && *s1 != '/')
        ++s1;
        

    // Find components in base directory that match the segment
    // pattern
    string head(u.begin(), s0);
    if(path(head).empty())
        head = replacePath(head, ".");
    string pattern(s0, s1);
    if(s1 != u.path.end())
        ++s1;
    string tail(s1, u.end());
    DirPtr dir = Directory::open(head);
    for(string mid; dir->read(mid); )
    {
        cerr << format("  test: %s -- %s : ") % reprString(wrap(pattern))
            % reprString(wrap(mid));

        if(0 != fnmatch(pattern.c_str(), mid.c_str(),
                        FNM_FILE_NAME | FNM_PERIOD))
        {
            cerr << "no\n";
            continue;
        }

        cerr << "match\n";


        string midPath = resolve(head, mid);
        string match = tail.empty() ? midPath : resolve(midPath, tail);
        cerr << format("  midpath=%s match=%s\n") %
            reprString(wrap(midPath)) % reprString(wrap(match));
        if(s1 == u.path.end())
        {
            cerr << "  path end\n";
            result.push_back(match);
        }
        else if(isDirectory(midPath))
        {
            cerr << "  recurse\n";
            globAppend(match, result);
        }
    }
}

#endif // globbing disabled


//----------------------------------------------------------------------------
// Auto-dispatch free wrappers
//----------------------------------------------------------------------------
bool fs::mkdir(string const & uri)
{
    return Filesystem::get(uri)->mkdir(uri);
}

bool fs::remove(string const & uri)
{
    return Filesystem::get(uri)->remove(uri);
}

void fs::rename(string const & sourceUri, string const & targetUri,
                bool overwrite)
{
    Filesystem::get(sourceUri)->rename(sourceUri, targetUri, overwrite);
}

size_t fs::filesize(string const & uri)
{
    return Filesystem::get(uri)->filesize(uri);
}

bool fs::exists(string const & uri)
{
    return Filesystem::get(uri)->exists(uri);
}
        
bool fs::isDirectory(string const & uri)
{
    return Filesystem::get(uri)->isDirectory(uri);
}

bool fs::isFile(string const & uri)
{
    return Filesystem::get(uri)->isFile(uri);
}

bool fs::isEmpty(string const & uri)
{
    return Filesystem::get(uri)->isEmpty(uri);
}

Timestamp fs::modificationTime(string const & uri)
{
    return Filesystem::get(uri)->modificationTime(uri);    
}

Timestamp fs::accessTime(string const & uri)
{
    return Filesystem::get(uri)->accessTime(uri);
}

Timestamp fs::creationTime(string const & uri)
{
    return Filesystem::get(uri)->creationTime(uri);
}

//----------------------------------------------------------------------------
// Hack for static libraries
//----------------------------------------------------------------------------
#include <warp/init.h>
WARP_DEFINE_INIT(warp_fs)
{
    // Get basic functionality
    WARP_CALL_INIT(warp_posixfs);
}
