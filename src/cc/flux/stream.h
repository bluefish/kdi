//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/flux/stream.h#1 $
//
// Created 2005/12/01
//
// Copyright 2005 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef FLUX_STREAM_H
#define FLUX_STREAM_H

#include "ex/exception.h"
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/noncopyable.hpp>
#include <string>


//----------------------------------------------------------------------------
// Stream typedef macro
//----------------------------------------------------------------------------
#define FLUX_STREAM_DEF(CLASS, SUPER)                   \
    typedef CLASS my_t;                                 \
    typedef SUPER super;                                \
    typedef boost::shared_ptr<my_t> handle_t;


/// Discrete data stream framework.  The basic object is the
/// Stream class.  Conceptually, there are input streams and
/// output streams, but they both derive from the same base class.
/// Input streams allow data to be pulled one element at a time.
/// Data can be pushed into output streams one element at a time.
/// Each Stream object is meant to implement some basic
/// functionality, such as serializing data to a file (Serialize),
/// eliminating duplicates in the stream (Uniq), or merging
/// multiple input streams into a single stream (Merge), for
/// example.  Streams can be composed into more complex flow
/// graphs in order to suit the needs of individual applications.
namespace flux
{
    //------------------------------------------------------------------------
    // StreamError
    //------------------------------------------------------------------------
    EX_DECLARE_EXCEPTION(StreamError, ex::Exception);


    //------------------------------------------------------------------------
    // Stream
    //------------------------------------------------------------------------
    /// Base class for flux discrete data stream framework.
    /// @param T Data type in stream
    template <class T>
    class Stream
        : public boost::enable_shared_from_this< Stream<T> >,
          private boost::noncopyable
    {
    public:
        typedef T value_t;
        typedef Stream<value_t> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

        typedef my_t base_t;
        typedef handle_t base_handle_t;

    protected:
        Stream() {}

    public:
        virtual ~Stream() {}


        // Record I/O

        /// Get the next value in the stream.
        /// @param[out] x The resulting value.
        /// @return True if there is a next element, false otherwise.
        /// @exception StreamError called get() on a non-input stream.
        ///
        /// This is the basic input operation on a Stream.
        virtual bool get(T & x)
        {
            ex::raise<StreamError>("get from non-input stream");
        }
        
        /// Put a value in the stream.
        /// @param[in] x The value to be put.
        /// @exception StreamError called put() on a non-output stream.
        ///
        /// This is the basic output operation on a Stream.
        virtual void put(T const & x)
        {
            ex::raise<StreamError>("put to non-output stream");
        }


        // Segmented I/O

        /// Fetch the next segment of a segmented input stream.
        /// @return True if there is more input available.
        virtual bool fetch() { return false; } // Load next segment

        /// Flush stream output
        virtual void flush() {}                // Finish current segment


        // Stream chaining

        /// Pipe output of this stream to another stream.
        /// @param output Stream to receive output.
        virtual void pipeTo(base_handle_t const & output)
        {
            ex::raise<StreamError>("output chaining unsupported");
        }

        /// Draw input for this stream from another stream.
        /// @param input Stream to provide input.
        virtual void pipeFrom(base_handle_t const & input)
        {
            ex::raise<StreamError>("input chaining unsupported");
        }

        // Diagnostic information

        /// Get a human-readable identifier for the stream.  This is
        /// intended for diagnostic purposes, and is not always
        /// available.
        virtual std::string getName() const
        {
            return std::string();
        }

        // Composite hooks

        /// Get a handle to a channel from a Composite Stream.
        /// @param idx Channel index to return
        virtual base_handle_t getChannel(int idx)
        {
            ex::raise<StreamError>("composite channels unsupported");
        }
    };


    //------------------------------------------------------------------------
    // copyStream
    //------------------------------------------------------------------------
    template <class T>
    void copyStream(Stream<T> & in, Stream<T> & out)
    {
        T x;
        do
        {
            while(in.get(x))
                out.put(x);
            out.flush();
        } while(in.fetch());
    }

    template <class T>
    Stream<T> & operator>>(Stream<T> & s1, Stream<T> & s2)
    {
        copyStream<T>(s1, s2);
        return s1;
    }
    template <class T>
    Stream<T> & operator<<(Stream<T> & s1, Stream<T> & s2)
    {
        copyStream<T>(s2, s1);
        return s1;
    }
}

#endif // FLUX_STREAM_H
