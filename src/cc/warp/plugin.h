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

#ifndef WARP_PLUGIN_H
#define WARP_PLUGIN_H

#include <ex/exception.h>
#include <boost/type_traits/function_traits.hpp>
#include <boost/preprocessor/repetition.hpp>
#include <boost/shared_ptr.hpp>
#include <string>
#include <dlfcn.h>

#ifndef WARP_PLUGIN_MAX_PARAMS
#  define WARP_PLUGIN_MAX_PARAMS 5
#endif

namespace warp {

    /// Plugin library loaded at runtime.
    class Plugin;

    /// Typed function loaded from a plugin.  The functor holds a
    /// reference to the Plugin so it can't be unloaded while the
    /// function reference is valid.
    template <class FuncType>
    class PluginFunctor;

} // namespace warp


//----------------------------------------------------------------------------
// Plugin
//----------------------------------------------------------------------------
class warp::Plugin
{
    boost::shared_ptr<void> library;

public:
    /// Open a shared library.
    explicit Plugin(std::string const & name)
    {
        using namespace ex;
        if(void * p = dlopen(name.c_str(), RTLD_NOW | RTLD_LOCAL))
            library.reset(p, dlclose);
        else
            raise<RuntimeError>("couldn't load library: %s",
                                dlerror());
    }

    /// Find a function pointer associated with the given name.  The
    /// symbol address is casted into a function pointer suiting the
    /// template parameter.  There is no type checking, so make sure
    /// you're right.  Returns null if the symbol cannot be located.
    template <class FuncType>
    FuncType * findFunction(std::string const & name) const
    {
        // These comments were stolen from Wang's wdswalker code.

        // C and C++ consider a cast from void * to a function pointer
        // undefined, so it's unfortunate that there is no dlsym(3)
        // variant that returns a function-pointer type.  IEEE and The
        // Open Group seem aware of the problem; in the meanwhile,
        // here are some workarounds...

        // http://standards.ieee.org/reading/ieee/interp/1003.1-2001/1003.1-2001-04.html
        // http://www.opengroup.org/onlinepubs/009695399/functions/dlsym.html

#ifdef HAVE_DLFUNC
        // A function returning a function-pointer type would allow a
        // "clean" cast.  (See dlfunc(3) on systems that have it.)
        return reinterpret_cast<FuncType *>(
            dlfunc(library.get(), name.c_str()));
#else
        // The dlsym(3) man pages for IEEE Std 1003.1, 2004 edition
        // ("POSIX.1") and GNU/Linux suggest this cast, but it
        // triggers a type-punning warning on gcc 4.1.1:
        //
        //   FuncType * func = 0;
        //   *(void **)(&func) = dlsym(library, name);
        //
        // Instead, we'll use a union to signal to the compiler that
        // the type-punning is intentional.  This supposedly disables
        // some sneakiness and suppresses the warning.
        union {
            void * sym;
            FuncType * func;
        } u;
        u.sym = dlsym(library.get(), name.c_str());
        return u.func;
#endif
    }

    /// Lookup a function and get a typed function pointer for it.
    /// The function type cannot be verified by the plugin loader, so
    /// make sure you get it right.  The caller is responsible for
    /// making sure that plugin is not unloaded before the returned
    /// function is called.  An exception is raised if the function
    /// cannot be found.
    template <class FuncType>
    FuncType * getFunction(std::string const & name) const
    {
        using namespace ex;
        FuncType * funcPtr = findFunction<FuncType>(name);
        if(!funcPtr)
            raise<RuntimeError>("couldn't find plugin function: %s",
                                name);
        return funcPtr;
    }

    /// Lookup a function and get a typed functor for it.  The
    /// function type cannot be verified by the plugin loader, so make
    /// sure you get it right.  The returned functor holds a reference
    /// to the Plugin, so the Plugin won't be unloaded while the
    /// functor exists.  An exception is raised if the function cannot
    /// be found.
    template <class FuncType>
    PluginFunctor<FuncType> getFunctor(std::string const & name) const;
};


//----------------------------------------------------------------------------
// PluginFunctor
//----------------------------------------------------------------------------
template <class FuncType>
class warp::PluginFunctor
{
public:
    typedef FuncType func_type;
    typedef typename boost::function_traits<FuncType>::result_type result_type;

private:
    Plugin plugin;
    FuncType * funcPtr;

public:
    /// Load the named function from the given plugin.
    PluginFunctor(Plugin const & plugin, std::string const & funcName) :
        plugin(plugin),
        funcPtr(plugin.getFunction<FuncType>(funcName)) {}

    /// Load the named plugin, then load the named function from it.
    PluginFunctor(std::string const & pluginName,
                  std::string const & funcName) :
        plugin(pluginName),
        funcPtr(plugin.getFunction<FuncType>(funcName)) {}

    /// Get the Plugin associated with this functor.
    Plugin const & getPlugin() const { return plugin; }
    
    // Call operator for 0 arguments
    result_type operator()() const
    {
        return (*funcPtr)();
    }

    // Call operator for 1 to WARP_PLUGIN_MAX_PARAMS arguments
    #undef WARP_PLUGIN_FUNCTOR_CALL
    #define WARP_PLUGIN_FUNCTOR_CALL(z,n,unused)        \
                                                        \
    template <BOOST_PP_ENUM_PARAMS(n, class A)>         \
    result_type operator()(                             \
        BOOST_PP_ENUM_BINARY_PARAMS(n, A, const & a)    \
        ) const                                         \
    {                                                   \
        return (*funcPtr)(BOOST_PP_ENUM_PARAMS(n, a));  \
    }

    BOOST_PP_REPEAT_FROM_TO(1, BOOST_PP_ADD(WARP_PLUGIN_MAX_PARAMS, 1),
                            WARP_PLUGIN_FUNCTOR_CALL, ~)
    #undef WARP_PLUGIN_FUNCTOR_CALL
};


//----------------------------------------------------------------------------
// Plugin methods
//----------------------------------------------------------------------------
namespace warp {

    template <class FuncType>
    PluginFunctor<FuncType>
    Plugin::getFunctor(std::string const & name) const
    {
        return PluginFunctor<FuncType>(*this, name);
    }

}


#endif // WARP_PLUGIN_H
