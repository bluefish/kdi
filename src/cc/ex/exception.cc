//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2005 Josh Taylor (Kosmix Corporation)
// Created 2005-12-05
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

#include "exception.h"
#include <execinfo.h>
#include <cstdio>
#include <algorithm>
#include <errno.h>
#include <string.h>
#include <cxxabi.h>
#include <boost/utility.hpp>

using namespace ex;

//----------------------------------------------------------------------------
// TerminateHandler
//----------------------------------------------------------------------------
namespace
{
    template <class T>
    class scoped_free : private boost::noncopyable
    {
        T * ptr;
    public:
        explicit scoped_free(T * p = NULL) :
            ptr(p)
        {}
        ~scoped_free() {
            if(ptr != NULL) 
                free(ptr);
        }
        T & operator*() const { return *ptr; }
        T * operator->() const { return ptr; }
    };

    class TerminateHandler
    {
        void (*oldTerminate)();

        TerminateHandler() :
            oldTerminate(0)
        {
            //std::cerr << "installing ex::TerminateHandler" << std::endl;
            oldTerminate = std::set_terminate(&handleTerminate);
        }

        ~TerminateHandler()
        {
            //std::cerr << "uninstalling ex::TerminateHandler" << std::endl;
            std::set_terminate(oldTerminate);
        }

        static void handleTerminate()
        {
            using std::cerr;
            using std::endl;

            if(std::type_info * t = abi::__cxa_current_exception_type())
            {
                char const * mangledName = t->name();
                int status = -1;
                char * demangledName =
                    abi::__cxa_demangle(mangledName, 0, 0, &status);
                scoped_free<char> _freeMe(demangledName);
                
                cerr << "terminate called after throwing: "
                     << (status == 0 ? demangledName : mangledName)
                     << endl;

                try { throw; }
                catch(Exception const & ex) {
                    cerr << "  what(): " << ex.what() << endl;
                    cerr << ex.getBacktrace();
                }
                catch(std::exception const & ex) {
                    cerr << "  what(): " << ex.what() << endl;
                }
                catch(...) {}
            }
            else
            {
                cerr << "terminate called without an active exception"
                     << endl;
            }
            abort();
        }

    public:
        static TerminateHandler const & get()
        {
            static TerminateHandler h;
            return h;
        }
    };
}

//----------------------------------------------------------------------------
// Backtrace
//----------------------------------------------------------------------------
Backtrace::Backtrace() :
    traceSize(0)
{
    TerminateHandler::get();
    //std::cerr << "Building backtrace" << std::endl;
    traceSize = backtrace(trace, MAX_BACKTRACE);
}

namespace
{
    void writeTraceLine(std::ostream & out, char const * line)
    {
        char const * end = line + strlen(line);

        // Parse lines that looks like:
        //   /data1/home/josh/cosmix.main/build/cc/release/ex/libex.so(_ZN2ex9BacktraceC1Ev+0x69) [0x2aaaaaaae9d9]
        //   build/cc/release/ex/trace_test(_ZN2ex5raiseINS_12RuntimeErrorEPKcEEvT0_+0x7d) [0x407e0d]
        //   build/cc/release/ex/trace_test(main+0) [0x402ef0]
        //   build/cc/release/ex/trace_test(main+0x9) [0x402ef9]
        //   /lib64/libc.so.6(__libc_start_main+0xf4) [0x3e0a11ce54]
        //   build/cc/release/ex/trace_test(_ZNSt15basic_streambufIcSt11char_traitsIcEE6xsputnEPKcl+0x41) [0x402de9]
        //
        // Parse into the following sections:
        //   exe    : executable or library name
        //   func   : mangled name of function
        //   offset : hex offset into the function
        //   addr   : global offset within executable
        
        char const * exeBegin    = line;
        char const * exeEnd      = std::find(exeBegin, end, '(');
        char const * funcBegin   = exeEnd == end ? end : exeEnd + 1;
        char const * funcEnd     = std::find(funcBegin, end, '+');
        char const * offsetBegin = funcEnd == end ? end : funcEnd + 1;
        char const * offsetEnd   = std::find(offsetBegin, end, ')');
        char const * addrBegin   = std::find(offsetEnd, end, '[');
        if(addrBegin != end)       ++addrBegin;
        char const * addrEnd     = std::find(addrBegin, end, ']');

        // Copy function name into a temporary buffer for demangling
        size_t funcLen = funcEnd - funcBegin;
        char funcBuf[funcLen+1];
        memcpy(funcBuf, funcBegin, funcLen);
        funcBuf[funcLen] = 0;

        // Demangle function name
        int status = -1;
        char * demangled = abi::__cxa_demangle(funcBuf, 0, 0, &status);
        scoped_free<char> _freeMe(demangled);
        char const * funcName = status == 0 ? demangled : funcBuf;

        // Write output line of the form:
        //   FUNCTION + OFFSET in LOCATION [ADDRESS]

        out << funcName << " + ";
        out.write(offsetBegin, offsetEnd - offsetBegin);
        out << " in ";
        out.write(exeBegin, exeEnd - exeBegin);
        out << " [";
        out.write(addrBegin, addrEnd - addrBegin);
        out << "]";
    }
}

void Backtrace::dump(std::ostream & out) const
{
    out << "Backtrace:" << std::endl;

    char ** symbols = NULL;
    if(!traceSize)
        return;

    symbols = backtrace_symbols(trace, traceSize);
    scoped_free<char *> _freeMe(symbols);
    if(symbols == NULL)
        return;

    for(int i = 0; i < traceSize; ++i)
    {
        out.width(5);
        out << (i+1) << ": ";
        writeTraceLine(out, symbols[i]);
        out << std::endl;
    }
}

std::ostream & ex::operator<<(std::ostream & out, Backtrace const & bt)
{
    bt.dump(out);
    return out;
}


//----------------------------------------------------------------------------
// Exception
//----------------------------------------------------------------------------
std::string Exception::toString() const
{
    std::string s(type());
    s += msg;
    return s;
}

std::ostream & ex::operator<<(std::ostream & out, Exception const & ex)
{
    out << ex.type() << ": " << ex.what();
    return out;
}

std::string ex::getStdError()
{
    return getStdError(errno);
}

std::string ex::getStdError(int errnum)
{
    // GNU has some wacky version of strerror_r that inverts the
    // meaning of the return code when compared to zero.  This code
    // originally assumed the POSIX version which caused it to always
    // fail.  It has been rewritten to use the GNU version and capture
    // the result in a pointer, so the compiler should complain if
    // someday the prototype switches to the POSIX version.
    //
    // From <string.h>:
    //
    // Reentrant version of `strerror'.  There are 2 flavors of
    // `strerror_r', GNU which returns the string and may or may not
    // use the supplied temporary buffer and POSIX one which fills the
    // string into the buffer.  To use the POSIX version,
    // -D_XOPEN_SOURCE=600 or -D_POSIX_C_SOURCE=200112L without
    // -D_GNU_SOURCE is needed, otherwise the GNU version is
    // preferred.

    char buf[4096];
    if(char * errstr = strerror_r(errnum, buf, sizeof(buf)))
        return std::string(errstr);
    else
    {
        snprintf(buf, sizeof(buf), "<unknown error: %d>", errnum);
        return std::string(buf);
    }
}
