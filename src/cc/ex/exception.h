//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2005 Josh Taylor (Kosmix Corporation)
// Created 2005-12-01
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

#ifndef EX_EXCEPTION_H
#define EX_EXCEPTION_H

#include <string>
#include <iostream>
#include <exception>
#include <boost/format.hpp>

#define EX_DECLARE_EXCEPTION(NAME, PARENT)                              \
    class NAME : public PARENT                                          \
    {                                                                   \
    public:                                                             \
        typedef PARENT super;                                           \
        NAME() {}                                                       \
        explicit NAME(std::string const & msg) : super(msg) {}          \
        virtual char const * type() const { return #NAME; }             \
    }

#ifdef __GNUC__
#define EX_NORETURN __attribute__ ((noreturn))
#else
#define EX_NORETURN
#endif

#ifndef NDEBUG
#define EX_REPORT(x) do {                               \
        std::cerr << "throw: " << (x) << std::endl;     \
    } while(0)
#else
#define EX_REPORT(x) (void)0
#endif


/// Exception handling framework.
namespace ex
{
    //------------------------------------------------------------------------
    // Backtrace
    //------------------------------------------------------------------------
    /// Encapsulation of the stack backtrace for the current thread.
    /// The backtrace is initialized when this object is constructed.
    class Backtrace
    {
        enum { MAX_BACKTRACE = 50 };

        void * trace[MAX_BACKTRACE];
        int traceSize;

    public:
        Backtrace();

        /// Get the stack backtrace as a raw array of pointers
        void * const * getBacktrace() const { return trace; }

        /// Get the size of the stack backtrace
        int getBacktraceSize() const { return traceSize; }

        /// Dump backtrace information to an ostream
        void dump(std::ostream & out) const;
    };

    std::ostream & operator<<(std::ostream & out, Backtrace const & bt);
    

    //------------------------------------------------------------------------
    // Exception
    //------------------------------------------------------------------------
    /// Base exception class.
    class Exception : public std::exception
    {
        std::string msg;
        Backtrace trace;

    public:
        Exception() {}
        explicit Exception(std::string const & msg) : msg(msg) {}
        virtual ~Exception() throw() {}

        virtual char const * type() const { return "Exception"; }
        virtual char const * what() const throw() { return msg.c_str(); }

        Backtrace const & getBacktrace() const { return trace; }
        
        std::string toString() const;
    };

    std::ostream & operator<<(std::ostream & out, Exception const & ex);


    //------------------------------------------------------------------------
    // SystemExit
    //------------------------------------------------------------------------
    class SystemExit : public Exception
    {
        int status;

    public:
        explicit SystemExit(int status) :
            Exception((boost::format("status=%d") % status).str()),
            status(status)
        {
        }
        virtual char const * type() const { return "SystemExit"; }

        int exitStatus() const { return status; }
    };


    //------------------------------------------------------------------------
    // raise
    //------------------------------------------------------------------------
    /// Raise an exception with a formatted error message.
    template <class Ex, class Str>
    EX_NORETURN void raise(Str fmt)
    {
        boost::format fmter(fmt);
        fmter.exceptions(boost::io::no_error_bits);
        Ex ex(fmter.str());
        EX_REPORT(ex);
        throw ex;
    }

    /// Raise an exception with a formatted error message.
    template <class Ex, class Str, class A1>
    EX_NORETURN void raise(Str fmt,
                           A1 const & a1)
    {
        boost::format fmter(fmt);
        fmter.exceptions(boost::io::no_error_bits);
        fmter % a1;
        Ex ex(fmter.str());
        EX_REPORT(ex);
        throw ex;
    }

    /// Raise an exception with a formatted error message.
    template <class Ex, class Str, class A1, class A2>
    EX_NORETURN void raise(Str fmt,
                           A1 const & a1, A2 const & a2)
    {
        boost::format fmter(fmt);
        fmter.exceptions(boost::io::no_error_bits);
        fmter % a1 % a2;
        Ex ex(fmter.str());
        EX_REPORT(ex);
        throw ex;
    }

    /// Raise an exception with a formatted error message.
    template <class Ex, class Str, class A1, class A2, class A3>
    EX_NORETURN void raise(Str fmt,
                           A1 const & a1, A2 const & a2, A3 const & a3)
    {
        boost::format fmter(fmt);
        fmter.exceptions(boost::io::no_error_bits);
        fmter % a1 % a2 % a3;
        Ex ex(fmter.str());
        EX_REPORT(ex);
        throw ex;
    }

    /// Raise an exception with a formatted error message.
    template <class Ex, class Str, class A1, class A2, class A3, class A4>
    EX_NORETURN void raise(Str fmt,
                           A1 const & a1, A2 const & a2, A3 const & a3,
                           A4 const & a4)
    {
        boost::format fmter(fmt);
        fmter.exceptions(boost::io::no_error_bits);
        fmter % a1 % a2 % a3 % a4;
        Ex ex(fmter.str());
        EX_REPORT(ex);
        throw ex;
    }

    /// Raise an exception with a formatted error message.
    template <class Ex, class Str, class A1, class A2, class A3, class A4,
              class A5>
    EX_NORETURN void raise(Str fmt,
                           A1 const & a1, A2 const & a2, A3 const & a3,
                           A4 const & a4, A5 const & a5)
    {
        boost::format fmter(fmt);
        fmter.exceptions(boost::io::no_error_bits);
        fmter % a1 % a2 % a3 % a4 % a5;
        Ex ex(fmter.str());
        EX_REPORT(ex);
        throw ex;
    }

    /// Raise an exception with a formatted error message.
    template <class Ex, class Str, class A1, class A2, class A3, class A4,
              class A5, class A6>
    EX_NORETURN void raise(Str fmt,
                           A1 const & a1, A2 const & a2, A3 const & a3,
                           A4 const & a4, A5 const & a5, A6 const & a6)
    {
        boost::format fmter(fmt);
        fmter.exceptions(boost::io::no_error_bits);
        fmter % a1 % a2 % a3 % a4 % a5 % a6;
        Ex ex(fmter.str());
        EX_REPORT(ex);
        throw ex;
    }

    /// Raise an exception with a formatted error message.
    template <class Ex, class Str, class A1, class A2, class A3, class A4,
              class A5, class A6, class A7>
    EX_NORETURN void raise(Str fmt,
                           A1 const & a1, A2 const & a2, A3 const & a3,
                           A4 const & a4, A5 const & a5, A6 const & a6,
                           A7 const & a7)
    {
        boost::format fmter(fmt);
        fmter.exceptions(boost::io::no_error_bits);
        fmter % a1 % a2 % a3 % a4 % a5 % a6 % a7;
        Ex ex(fmter.str());
        EX_REPORT(ex);
        throw ex;
    }

    /// Raise an exception with a formatted error message.
    template <class Ex, class Str, class A1, class A2, class A3, class A4,
              class A5, class A6, class A7, class A8>
    EX_NORETURN void raise(Str fmt,
                           A1 const & a1, A2 const & a2, A3 const & a3,
                           A4 const & a4, A5 const & a5, A6 const & a6,
                           A7 const & a7, A8 const & a8)
    {
        boost::format fmter(fmt);
        fmter.exceptions(boost::io::no_error_bits);
        fmter % a1 % a2 % a3 % a4 % a5 % a6 % a7 % a8;
        Ex ex(fmter.str());
        EX_REPORT(ex);
        throw ex;
    }

    /// Raise an exception with a formatted error message.
    template <class Ex, class Str, class A1, class A2, class A3, class A4,
              class A5, class A6, class A7, class A8, class A9>
    EX_NORETURN void raise(Str fmt,
                           A1 const & a1, A2 const & a2, A3 const & a3,
                           A4 const & a4, A5 const & a5, A6 const & a6,
                           A7 const & a7, A8 const & a8, A9 const & a9)
    {
        boost::format fmter(fmt);
        fmter.exceptions(boost::io::no_error_bits);
        fmter % a1 % a2 % a3 % a4 % a5 % a6 % a7 % a8 % a9;
        Ex ex(fmter.str());
        EX_REPORT(ex);
        throw ex;
    }

    /// Raise an exception with a formatted error message.
    template <class Ex, class Str, class A1, class A2, class A3, class A4,
              class A5, class A6, class A7, class A8, class A9, class A10>
    EX_NORETURN void raise(Str fmt,
                           A1 const & a1, A2 const & a2, A3 const & a3,
                           A4 const & a4, A5 const & a5, A6 const & a6,
                           A7 const & a7, A8 const & a8, A9 const & a9,
                           A10 const & a10)
    {
        boost::format fmter(fmt);
        fmter.exceptions(boost::io::no_error_bits);
        fmter % a1 % a2 % a3 % a4 % a5 % a6 % a7 % a8 % a9 % a10;
        Ex ex(fmter.str());
        EX_REPORT(ex);
        throw ex;
    }

    //------------------------------------------------------------------------
    // Common exceptions
    //------------------------------------------------------------------------
    EX_DECLARE_EXCEPTION(IOError, Exception);
    EX_DECLARE_EXCEPTION(StopIteration, Exception);
    EX_DECLARE_EXCEPTION(RuntimeError, Exception);
    EX_DECLARE_EXCEPTION(NotImplementedError, RuntimeError);
    EX_DECLARE_EXCEPTION(LookupError, Exception);
    EX_DECLARE_EXCEPTION(KeyError, LookupError);
    EX_DECLARE_EXCEPTION(IndexError, LookupError);
    EX_DECLARE_EXCEPTION(ValueError, Exception);
    EX_DECLARE_EXCEPTION(NullPointerError, ValueError);

    //------------------------------------------------------------------------
    // Utility
    //------------------------------------------------------------------------
    /// Get last system error string
    std::string getStdError();
    /// Get system error string for the given error code
    std::string getStdError(int errnum);
}

//----------------------------------------------------------------------------
// Null pointer check
//----------------------------------------------------------------------------
#define EX_CHECK_NULL(x) do {                   \
    if(!(x))                                    \
        ex::raise<ex::NullPointerError>(#x);    \
} while(0)


//----------------------------------------------------------------------------
// Unimplemented function exception
//----------------------------------------------------------------------------
#ifdef __GNUC__

#define EX_UNIMPLEMENTED_FUNCTION do {                                  \
    ex::raise<ex::NotImplementedError>("%s(%d): not implemented: %s",   \
                                       __FILE__, __LINE__,              \
                                       __PRETTY_FUNCTION__);            \
} while(0)

#else

#define EX_UNIMPLEMENTED_FUNCTION do {                                  \
    ex::raise<ex::NotImplementedError>("%s(%d): not implemented",       \
                                       __FILE__, __LINE__);             \
} while(0)

#endif

#endif // EX_EXCEPTION_H
