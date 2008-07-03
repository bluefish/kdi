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

#ifndef WARP_CONFIG_H
#define WARP_CONFIG_H

#include <string>
#include <vector>
#include <utility>
#include <map>
#include <boost/shared_ptr.hpp>
#include <boost/lexical_cast.hpp>

namespace warp
{
    class Config;

    // Forward declaration
    class File;
    typedef boost::shared_ptr<File> FilePtr;
}

//----------------------------------------------------------------------------
// Config
//----------------------------------------------------------------------------
class warp::Config
{
    std::string key;
    std::string value;
    std::vector<Config *> children;
    std::map<std::string, Config *> childMap;
    std::string baseUri;

public:
    Config();
    ~Config();

    explicit Config(FilePtr const & fp);
    explicit Config(std::string const & fn);

    // Deep copy
    Config(Config const & o);
    Config const & operator=(Config const & o);

    /// Find the config node for the given relative key.  A null
    /// pointer is returned if the node does not exist.
    Config const * findChild(std::string const & key) const;

private:
    /// Find the config or create node for the given relative key.
    Config * findOrMakeChild(std::string const & key);

public:
    /// Get the config node for the given relative key.  If the node
    /// does not exist, a KeyError is thrown.
    Config const & getChild(std::string const & key) const;

    /// Get a child config node by index.  The child node must exist.
    /// If it does not, an IndexError is thrown.
    Config const & getChild(size_t idx) const;

    /// Get the key at this config node.  The returned name is the
    /// leaf name on the path, not the fully qualified key.  For
    /// example, on a node accessed through "foo.bar.baz", this
    /// function would return "baz".
    std::string const & getKey() const;

    /// Get the fully qualified key at this config node.  The key is
    /// derived from the node's base URI and its key.  Since the base
    /// URI isn't always meaningful, the results of this function can
    /// be suspect.  It is provided for debugging convenience.
    std::string getFullKey() const;

    /// Get a URI for the Config rooted at this node.  The URI is
    /// derived from the File name used when this tree.  If the File
    /// did not provide a useful URI, this function will not either.
    std::string const & getBaseUri() const;

    /// Get the URI for the Config rooted at the given relative key.
    /// The URI is derived from the File name used when this tree.  If
    /// the File did not provide a useful URI, this function will not
    /// either.
    std::string const & getBaseUri(std::string const & key) const;

    /// Get the value at this config node.
    std::string const & get() const;

    /// Get the value for the given relative key.  The referenced node
    /// must exist.
    std::string const & get(std::string const & key) const;

    /// Get the value for the given relative key.  If the referenced
    /// node does not exist, the given default value is used.
    std::string const & get(
        std::string const & key, std::string const & dflt) const;

    /// Get the value at this config node, interpreted as the
    /// specified type.
    template <class T>
    T getAs() const
    {
        return boost::lexical_cast<T>(get());
    }

    /// Get the value for the given relative key, interpreted as the
    /// specified type.  The referenced node must exist.
    template <class T>
    T getAs(std::string const & key) const
    {
        return getChild(key).getAs<T>();
    }

    /// Get the value for the given relative key, interpreted as the
    /// specified type.  If the referenced node does not exist, the
    /// given default value is used.
    template <class T>
    T getAs(std::string const & key, T const & dflt) const
    {
        if(Config const * n = findChild(key))
            return n->getAs<T>();
        else
            return dflt;
    }

    /// Get the value at this config node interpreted as a URI
    /// reference.  The URI reference is resolved relative to the
    /// node's base URI.
    /// @see getBaseUri()
    std::string getAsUri() const;

    /// Get the value for the given relative key interpreted as a URI
    /// reference.  The URI reference is resolved relative to the
    /// node's base URI.  The referenced node must exist.
    /// @see getBaseUri()
    std::string getAsUri(std::string const & key) const;

    /// Get the value for the given relative key interpreted as a URI
    /// reference.  The URI reference is resolved relative to the
    /// node's base URI.  If the referenced node does not exist, the
    /// given default reference is used.  If resolveDflt is true, the
    /// given default is also resolved relative to the config file's
    /// base URI.  Otherwise it is returned as-is.
    /// @see getBaseUri()
    std::string getAsUri(std::string const & key, std::string const & dflt,
                         bool resolveDflt) const;

    /// Get the values of the children under this node.
    std::vector<std::string> getValues() const;

    /// Get the values of the children under the given relative key.
    /// The referenced node must exist.
    std::vector<std::string> getValues(std::string const & key) const;

    /// Set a value for this node.
    void set(std::string const & value);

    /// Set a value for the given relative key.  If the node doesn't
    /// already exist, it will be created.
    void set(std::string const & key, std::string const & value);

    /// Clear subtree below this node.
    void clear();

    /// Return number of children
    size_t numChildren() const;

    /// Load config variables from a properties file.  If the file URI
    /// specifies a "configRoot" parameter, it will be used as the
    /// root of the tree and the rest will be discarded.
    void loadProperties(FilePtr const & fp);

    /// Load config variables from a properties file.  If the file URI
    /// specifies a "configRoot" parameter, it will be used as the
    /// root of the tree and the rest will be discarded.
    void loadProperties(std::string const & fn);

    /// Output config variables in properties file format.
    void writeProperties(FilePtr const & fp) const;

    /// Output config variables in properties file format.
    void writeProperties(std::string const & fn) const;
};

namespace warp
{
    /// Output config variables in properties file format.
    std::ostream & operator<<(std::ostream & out, Config const & cfg);
}

#endif // WARP_CONFIG_H
