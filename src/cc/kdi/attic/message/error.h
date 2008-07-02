//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/attic/message/error.h#1 $
//
// Created 2007/11/21
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_MESSAGE_ERROR_H
#define KDI_MESSAGE_ERROR_H

#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>

namespace kdi {
namespace message {

    /// Base class for a rethrowable exception 
    class Error : private boost::noncopyable
    {
    public:
        virtual ~Error() {}

        /// Rethrow the exception
        virtual void rethrow() const = 0;

        /// Get the message associated with the exception
        virtual char const * what() const = 0;
    };

    /// Class for rethrowing specific exception types
    template <class T>
    class TypedError : public Error
    {
        T ex;
    public:
        explicit TypedError(T const & ex) : ex(ex) {}
        virtual void rethrow() const { throw ex; }
        virtual char const * what() const { return ex.what(); }
    };

    /// Shared pointer to an Error
    typedef boost::shared_ptr<Error const> ErrorPtr;

    /// Wrap an exception in an ErrorPtr for later use
    template <class E>
    ErrorPtr wrapError(E const & ex)
    {
        ErrorPtr p(new TypedError<E>(ex));
        return p;
    }

} // namespace message
} // namespace kdi

#endif // KDI_MESSAGE_ERROR_H
