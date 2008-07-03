//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-05-14
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

#include "config.h"
#include "util.h"
#include "strutil.h"
#include "file.h"
#include "filestream.h"
#include "uri.h"
#include "ex/exception.h"

using namespace warp;
using namespace ex;
using namespace std;

namespace
{
    /// Return true if the given range is a valid key
    template <class It>
    bool checkKey(It begin, It end)
    {
        for(;;)
        {
            // Must have a valid identifier start
            if(begin == end || (!isalpha(*begin) && *begin != '_'))
                return false;

            // Rest of the segment must be an identifier
            for(++begin; begin != end && *begin != '.'; ++begin)
            {
                if(!isalnum(*begin) && *begin != '_')
                    return false;
            }

            // If we're at end of string then it's good
            if(begin == end)
                return true;

            // Otherwise we found a segment separator
            ++begin;
        }
    }

    /// Throw an exception if \c is not a valid key
    void validateKey(string const & key)
    {
        if(!checkKey(key.begin(), key.end()))
            raise<ValueError>("invalid key: %s", reprString(wrap(key)));
    }

    /// Get the first identifer component in a key
    string keyHead(string const & key)
    {
        return key.substr(0, key.find('.'));
    }

    /// Get the (possibly empty) tail of a key after removing the head
    /// component
    string keyTail(string const & key)
    {
        return key.substr(key.find('.')+1);
    }

    /// Given a base URI, which may or may not contain a configRoot
    /// parameter in the query section, return a URI with a configRoot
    /// parameter that points to the given relative key.  If the base
    /// has a configRoot, the key is taken to be relative to that
    /// base.  Otherwise the key is taken as the new root.
    string resolveConfigUri(string const & baseUri, string const & relKey)
    {
        // Get existing root
        string root = uriGetParameter(baseUri, "configRoot");

        // Update for new relative key
        if(!root.empty())
            root += '.';
        root += relKey;

        // Replace root
        return uriSetParameter(baseUri, "configRoot", root);
    }
}

//----------------------------------------------------------------------------
// Config
//----------------------------------------------------------------------------
Config::Config()
{
}

Config::Config(FilePtr const & fp)
{
    loadProperties(fp);
}
    
Config::Config(string const & fn)
{
    loadProperties(fn);
}

Config::~Config()
{
    for_each(children.begin(), children.end(), delete_ptr());
}

Config::Config(Config const & o) :
    key(o.key), value(o.value), baseUri(o.baseUri)
{
    for(vector<Config *>::const_iterator i = o.children.begin();
        i != o.children.end(); ++i)
    {
        children.push_back(new Config(**i));
        childMap[(*i)->key] = children.back();
    }
}

Config const & Config::operator=(Config const & o)
{
    if(&o == this)
        return *this;

    // Make a tmp copy, in case we're taking assignment from a child
    // node.  We don't want to delete the child before the copy.
    Config tmp(o);

    // Clear self
    clear();

    // Copy from tmp
    key = tmp.key;
    value = tmp.value;
    baseUri = tmp.baseUri;

    for(vector<Config *>::const_iterator i = tmp.children.begin();
        i != tmp.children.end(); ++i)
    {
        children.push_back(new Config(**i));
        childMap[(*i)->key] = children.back();
    }
    
    return *this;
}

Config const * Config::findChild(string const & keyRef) const
{
    // Validate key
    validateKey(keyRef);

    // Search tree for key
    Config const * cfg = this;
    string key(keyRef);
    for(;;)
    {
        // Get the first segment and look it up in the child map
        string head = keyHead(key);
        map<string, Config *>::const_iterator i = cfg->childMap.find(head);

        // Check for existence
        if(i == cfg->childMap.end())
            return 0;

        // If the head is the whole key, we're done
        if(head.size() == key.size())
            return i->second;

        // Otherwise recurse
        key = keyTail(key);
        cfg = i->second;
    }
}

Config * Config::findOrMakeChild(std::string const & keyRef)
{
    // Validate key
    validateKey(keyRef);

    // Search for key node, creating path as necessary
    Config * cfg = this;
    string key(keyRef);
    for(;;)
    {
        // Get the first segment and look it up in the child map
        string head = keyHead(key);
        Config * & child = cfg->childMap[head];

        // If it doesn't exist, create it
        if(!child)
        {
            child = new Config;
            child->key = head;
            child->baseUri = resolveConfigUri(cfg->baseUri, head);
            cfg->children.push_back(child);
        }

        // If the head is the whole key, we've found the node we want
        if(head == key)
            return child;

        // Otherwise recurse
        key = keyTail(key);
        cfg = child;
    }
}

Config const & Config::getChild(string const & key) const
{
    if(Config const * n = findChild(key))
        return *n;
    else
    {
        string fullKey = uriGetParameter(getBaseUri(), "configRoot");
        if(!fullKey.empty())
            fullKey += '.';
        fullKey += key;
        raise<KeyError>("missing key: %s (file=%s)", fullKey, getBaseUri());
    }
}

Config const & Config::getChild(size_t idx) const
{
    if(idx >= children.size())
        raise<IndexError>("child index out of bounds: %d (file=%s)",
                          idx, getBaseUri());

    return *children[idx];
}

string const & Config::getKey() const
{
    return key;
}

string Config::getFullKey() const
{
    return uriGetParameter(getBaseUri(), "configRoot");
}

string const & Config::getBaseUri() const
{
    return baseUri;
}

string const & Config::getBaseUri(string const & key) const
{
    return getChild(key).getBaseUri();
}

string const & Config::get() const
{
    return value;
}

string const & Config::get(string const & key) const
{
    return getChild(key).get();
}

string const & Config::get(string const & key, string const & dflt) const
{
    if(Config const * n = findChild(key))
        return n->get();
    else
        return dflt;
}

string Config::getAsUri() const
{
    return uriResolve(getBaseUri(), get());
}

string Config::getAsUri(string const & key) const
{
    return getChild(key).getAsUri();
}

string Config::getAsUri(string const & key, string const & dflt, bool resolveDflt) const
{
    if(Config const * n = findChild(key))
        return n->getAsUri();
    else if(resolveDflt)
        return uriResolve(baseUri, dflt);
    else
        return dflt;
}

vector<string> Config::getValues() const
{
    vector<string> r;
    for(vector<Config *>::const_iterator i = children.begin();
        i != children.end(); ++i)
    {
        r.push_back((*i)->get());
    }
    return r;
}

vector<string> Config::getValues(string const & key) const
{
    return getChild(key).getValues();
}

void Config::set(string const & value)
{
    this->value = value;
}

void Config::set(string const & key, string const & value)
{
    findOrMakeChild(key)->set(value);
}

void Config::clear()
{
    key.clear();
    value.clear();
    for_each(children.begin(), children.end(), delete_ptr());
    children.clear();
    childMap.clear();
    baseUri.clear();
}

size_t Config::numChildren() const
{
    return children.size();
}

namespace
{
    char const * skipSpace(char const * begin, char const * end)
    {
        for(; begin != end; ++begin)
        {
            if(!isspace(*begin))
                break;
        }
        return begin;
    }

    char const * skipKeyValueSeparator(char const * begin, char const * end)
    {
        bool haveSep = false;
        for(; begin != end; ++begin)
        {
            if(isspace(*begin))
                continue;

            if(!haveSep && (*begin == ':' || *begin == '='))
            {
                haveSep = true;
                continue;
            }

            break;
        }
        
        return begin;
    }

    char const * readKey(char const * begin, char const * end, string & key)
    {
        while(begin != end)
        {
            if(*begin == '\\')
            {
                ++begin;
                if(begin == end)
                    // Back off trailing backslash
                    return --begin;

                begin = decodeEscapeSequence(begin, end, key);
            }
            else if(isspace(*begin) || *begin == ':' || *begin == '=')
            {
                break;
            }
            else
            {
                key += *begin;
                ++begin;
            }
        }
        return begin;
    }

    bool readValue(char const * begin, char const * end, string & value)
    {
        while(begin != end)
        {
            if(*begin == '\\')
            {
                ++begin;
                if(begin == end)
                    // Trailing backslash
                    return true;

                begin = decodeEscapeSequence(begin, end, value);
            }
            else
            {
                value += *begin;
                ++begin;
            }
        }
        return false;
    }

    void chomp(char const * begin, char const * & end)
    {
        while(begin != end)
        {
            if(end[-1] != '\n' && end[-1] != '\r')
                break;
            --end;
        }
    }
}

void Config::loadProperties(FilePtr const & fp)
{
    if(!fp)
        raise<ValueError>("null file");

    // Erase old tree
    clear();

    // Get base URI from File name
    std::string loadUri = fp->getName();
    baseUri = uriEraseParameter(loadUri, "configRoot");

    // Read file
    char buf[16<<10];
    size_t len;
    size_t line = 0;
    while((len = fp->readline(buf, sizeof(buf))))
    {
        ++line;

        char const * begin = buf;
        char const * end = begin + len;

        // Chop newline
        chomp(begin, end);

        // Skip leading space
        begin = skipSpace(begin, end);

        // Skip comments and blank lines
        if(begin == end || *begin == '#' || *begin == '!')
            continue;

        // Read key
        string key;
        begin = readKey(begin, end, key);

        // Make sure it is a valid Config key
        if(!checkKey(key.begin(), key.end()))
            raise<ValueError>("invalid config key %s at line %d: %s",
                              reprString(wrap(key)), line, fp->getName());

        // Skip key/value separator
        begin = skipKeyValueSeparator(begin, end);

        // Read value (including multi-line continuations)
        string value;
        while(readValue(begin, end, value))
        {
            // Continue, read new line
            len = fp->readline(buf, sizeof(buf));
            if(!len)
                break;

            begin = buf;
            end = buf + len;

            // Chop newline
            chomp(begin, end);
            
            // Skip leading space
            begin = skipSpace(begin, end);
        }

        // Set the key
        set(key, value);
    }

    // If the URI has a configRoot parameter, re-root this tree at the
    // specified key.
    string root = uriGetParameter(loadUri, "configRoot");
    if(!root.empty())
        *this = getChild(uriDecode(root));
}

void Config::loadProperties(std::string const & fn)
{
    loadProperties(File::input(fn));
}

namespace
{
    void writeProperties(ostream & out, Config const & n, string const & outKey)
    {
        // Write values for leaf nodes and internal nodes with
        // non-empty values.
        string const & v = n.get();
        if(!outKey.empty() && (n.numChildren() == 0 || !v.empty()))
        {
            out << outKey << " = ";

            // If value starts with a space, prefix it with a
            // backslash so it won't get eaten up when loading.
            if(!v.empty() && v[0] == ' ')
                out << '\\';

            // Write escaped version of value string
            out << reprString(wrap(v), false) << endl;
        }

        // Write children in depth-first order
        for(size_t i = 0; i < n.numChildren(); ++i)
        {
            Config const & child = n.getChild(i);

            // Compose child key
            string childKey = outKey;
            if(!childKey.empty())
                childKey += '.';
            childKey += child.getKey();

            writeProperties(out, child, childKey);
        }
    }
}

void Config::writeProperties(FilePtr const & fp) const
{
    FileStream out(fp);
    out << *this;
}

void Config::writeProperties(string const & fn) const
{
    writeProperties(File::output(fn));
}

ostream & warp::operator<<(ostream & out, Config const & n)
{
    string outKey = uriGetParameter(n.getBaseUri(), "configRoot");
    if(outKey.empty())
       outKey = n.getKey();

    writeProperties(out, n, outKey);
    return out;
}
