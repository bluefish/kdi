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

#ifndef WARP_OPTIONS_H
#define WARP_OPTIONS_H

#include <ex/exception.h>
#include <boost/program_options.hpp>
#include <string>
#include <vector>
#include <set>

namespace warp
{
    // OptionError exception
    EX_DECLARE_EXCEPTION(OptionError, ex::Exception);

    class OptionParser;
    class OptionMap;
    class ArgumentList;

    using boost::program_options::value;
}


//----------------------------------------------------------------------------
// OptionMap
//----------------------------------------------------------------------------
class warp::OptionMap
    : public boost::program_options::variables_map
{
    typedef boost::program_options::variables_map super;
    std::set<std::string> tweaks;

public:
    OptionMap() {}
    OptionMap(super const & s, std::set<std::string> const & tweaks);

    bool get(char const * name, std::string & v) const
    {
        if(count(name))
        {
            v = boost::any_cast<std::string>((*this)[name].value());
            if(tweaks.find(v) != tweaks.end())
                v.erase(v.size()-1);
            return true;
        }
        else
            return false;
    }

    template <class V>
    bool get(char const * name, V & v) const
    {
        if(count(name))
        {
            v = boost::any_cast<V>((*this)[name].value());
            return true;
        }
        else
            return false;
    }
};

namespace warp
{
    template <class V>
    inline bool getopt(OptionMap const & m, char const * name, V & v)
    {
        return m.get(name, v);
    }

    inline bool hasopt(OptionMap const & m, char const * name)
    {
        return m.count(name) > 0;
    }
}


//----------------------------------------------------------------------------
// ArgumentList
//----------------------------------------------------------------------------
class warp::ArgumentList : public std::vector<std::string>
{
    typedef std::vector<std::string> super;

public:
    ArgumentList() {}
    ArgumentList(super const & s) : super(s) {}
};


//----------------------------------------------------------------------------
// OptionParser
//----------------------------------------------------------------------------
class warp::OptionParser
{
public:
    typedef OptionMap Options;
    typedef ArgumentList Arguments;
    typedef boost::program_options::value_semantic ValueSemantic;

private:
    typedef boost::program_options::options_description OptionDesc;

    OptionDesc optionDesc;
    std::string progName;
    std::string usageFmt;

public:
    /// Construct option parser with the given usage string.  The
    /// substring "%prog" will be replaced by the basename of the
    /// executable when options are parsed.
    explicit OptionParser(std::string const & usage = "%prog [options]",
                          std::string const & optCaption = "Options",
                          bool autoHelp = true);
    
    /// Add a basic option.
    /// @param[in] name Option name of the form: "longname,shortname"
    /// (e.g "verbose,v")
    /// @param[in] help Help text for option.
    void addOption(char const * name, char const * help);

    /// Add an option that takes a value.
    /// @param[in] name Option name of the form: "longname,shortname"
    /// (e.g. "verbose,v")
    /// @param[in] val Value semantic of option, in Boost
    /// program_option style.  This is admittedly awkward.  For
    /// example, if you want an option that has an int type value, use
    /// boost::program_options::value<int>().  If you want a string,
    /// use boost::program_options::value<std::string>().  If you want
    /// a default value, use
    /// boost::program_options::value<float>()->default_value(42.0f).
    /// See main() in wds/tarcat.cc for more examples.
    /// @param[in] help Help text for option.
    void addOption(char const * name, ValueSemantic const * val,
                   char const * help);

    /// Append a section of options.
    void addSection(OptionParser const & op);

    /// Parse command-line arguments, possibly throwing OptionError.
    /// The program name will determined here from \c av[0].  The
    /// resulting option map will be stored in \c opt and the
    /// remaining positional arguments will be stored in \c args.
    /// @param[in] ac Number of arguments in \c av.
    /// @param[in] av Argument vector.  The first element should be
    /// the invokation name.
    /// @param[out] opt Resulting option map.
    /// @param[out] args Resulting positional arguments.
    void parse(int ac, char ** av, Options & opt, Arguments & args);

    /// Parse command-line arguments, printing an error message and
    /// exiting if there are problems.
    /// The program name will determined here from \c av[0].  The
    /// resulting option map will be stored in \c opt and the
    /// remaining positional arguments will be stored in \c args.
    /// @param[in] ac Number of arguments in \c av.
    /// @param[in] av Argument vector.  The first element should be
    /// the invokation name.
    /// @param[out] opt Resulting option map.
    /// @param[out] args Resulting positional arguments.
    void parseOrBail(int ac, char ** av, Options & opt, Arguments & args);
    
    /// Get usage string.  If available, "%prog" will be replaced by
    /// the executable name in the usage string.
    /// @return Usage string.
    std::string getUsage() const;

    /// Set usage string.  May contain a "%prog" variable, which will
    /// be replaced by the executable name.
    void setUsage(std::string const & usage);

    /// Print error message and exit (if abortProgram is true)
    /// @param[in] message Error message to print.
    /// @param[in] abortProgram Exit program if this is true.
    void error(std::string const & message, bool abortProgram=true) const;
};



#endif // WARP_OPTIONS_H
