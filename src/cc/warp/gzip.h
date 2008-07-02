//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/gzip.h#1 $
//
// Created 2006/01/24
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_GZIP_H
#define WARP_GZIP_H

#include "ex/exception.h"
#include "buffer.h"
#include <algorithm>
#include <stdint.h>
#include <boost/crc.hpp>
#include <boost/utility.hpp>

#include <zlib.h>
#if ZLIB_VERNUM < 0x1230
#include <zutil.h>
#else
// We need OS_CODE
#define OS_CODE  0x03  /* assume Unix */ // http://mail-archives.apache.org/mod_mbox/httpd-dev/200410.mbox/%3C20041008145915.GD8385@redhat.com%3E
#endif

namespace warp
{
    EX_DECLARE_EXCEPTION(ZlibError, ex::Exception);
    EX_NORETURN void raiseZlibError(int zErr);

    /// Zlib wrapper for inflating zlib or gzip streams.
    /// @author Josh Taylor
    class GzInflater : public boost::noncopyable
    {
        ::z_stream zs;
        bool isDone;

    public:
        GzInflater() :
            isDone(true)
        {
            zs.zalloc = Z_NULL;
            zs.zfree = Z_NULL;
            zs.opaque = Z_NULL;
            zs.next_in = Z_NULL;
            zs.avail_in = 0;

            int ret = ::inflateInit2_(&zs, 15+32, ZLIB_VERSION,
                                          sizeof(::z_stream));
            if(ret != Z_OK)
                raiseZlibError(ret);
        }

        ~GzInflater()
        {
            ::inflateEnd(&zs);
        }

        /// Begin new decompression input stream.
        /// @param src Input buffer address
        /// @param len Input buffer length
        void setInput(void const * src, size_t len)
        {
            ::inflateReset(&zs);
            zs.next_in = (::Bytef *)src;
            zs.avail_in = len;
            isDone = false;
        }

        /// Continue decompression input stream after last input is
        /// exhausted.  That is, the input stream should be initiated
        /// with setInput(), and continueInput() should only be called
        /// if inputRemaining() is zero, and done() is not true.
        /// @param src Input buffer address
        /// @param len Input buffer length
        void continueInput(void const * src, size_t len)
        {
            assert(!done() && inputRemaining() == 0);
            zs.next_in = (::Bytef *)src;
            zs.avail_in = len;
        }

        /// Return true if decompression is complete.
        bool done() const
        {
            return isDone;
        }
        
        /// Return number of bytes left in current input stream.
        size_t inputRemaining() const
        {
            return zs.avail_in;
        }

        /// Inflate to a target character buffer, as much as possible.
        /// @param dst Destination buffer address
        /// @param dstLen Destination buffer length
        /// @return Number of output bytes generated
        size_t inflate(void * dst, size_t dstLen)
        {
            zs.next_out = (::Bytef *)dst;
            zs.avail_out = dstLen;

            int ret = ::inflate(&zs, Z_NO_FLUSH);
            switch(ret)
            {
                case Z_STREAM_END:  isDone = true;
                case Z_OK:          break;
                default:            raiseZlibError(ret);
            }

            return dstLen - zs.avail_out;
        }

        /// Inflate to a target Buffer, as much as possible.
        /// @param dst Destination Buffer
        /// @return Number of output bytes generated
        size_t inflate(Buffer & dst)
        {
            char * p = dst.position();
            size_t sz = inflate(p, dst.remaining());
            dst.position(p + sz);
            return sz;
        }

        /// Inflate to a target Buffer, resizing if necessary.
        /// @param dst Destination Buffer
        /// @param outputRatio Expected ratio of output size to input size
        /// @param growthFactor Minimum growth to use when resizing
        /// @return Number of output bytes generated
        size_t inflateAll(Buffer & dst, float outputRatio = 5.0f,
                          float growthFactor = 2.0f)
        {
            assert(growthFactor > 1.0f);

            size_t startSz = dst.consumed();
            size_t inLeft;
            while(!done() && (inLeft = inputRemaining()))
            {
                size_t outSz = size_t(inLeft * outputRatio);
                if(dst.remaining() < outSz)
                {
                    size_t newCap = std::max(
                        dst.consumed() + outSz,
                        size_t(dst.capacity() * growthFactor));
                    Buffer tmp(newCap);
                    dst.flip();
                    tmp.put(dst);
                    tmp.swap(dst);
                }
                inflate(dst);
            }
            
            return dst.consumed() - startSz;
        }

        /// Inflate to an output iterator.  Iterated type should
        /// support \c char assignment, and iterator should be able to
        /// accomodate all output data.
        /// @param out Output iterator for destination
        /// @return Output iterator position at end of output data
        template <class OutIt>
        OutIt inflateAll(OutIt out)
        {
            enum { BUF_SIZE = 16 << 10 };
            char outBuf[BUF_SIZE];

            while(size_t sz = inflate(outBuf, BUF_SIZE))
                out = std::copy(outBuf, outBuf + sz, out);

            return out;
        }
    };

    /// Inflate gzip or zlib data to an output iterator.  Iterated
    /// type should support \c char assignment, and iterator should be
    /// able to accomodate all output data.
    /// @param src Compressed input buffer address
    /// @param len Compressed input buffer length
    /// @param out Output iterator for destination
    /// @return Output iterator position at end of output data
    template <class OutIt>
    OutIt gunzip(void const * src, size_t len, OutIt out)
    {
        GzInflater gz;
        gz.setInput(src, len);
        return gz.inflateAll(out);
    }


    class GzDeflater
    {
        ::z_stream zs;
        bool isDone;
        bool isStarted;
        uint32_t fileSize;
        uint32_t fileCrc;
        void * src;

    public:
        GzDeflater() :
            isDone(true), isStarted(false)
        {
            zs.zalloc = Z_NULL;
            zs.zfree = Z_NULL;
            zs.opaque = Z_NULL;
            zs.next_in = Z_NULL;
            zs.avail_in = 0;

            int ret = ::deflateInit2_(&zs, 
                                      Z_DEFAULT_COMPRESSION, 
                                      Z_DEFLATED, 
                                      -MAX_WBITS, 
                                      MAX_MEM_LEVEL, 
                                      Z_DEFAULT_STRATEGY,
                                      ZLIB_VERSION,
                                      sizeof(::z_stream));
            
            if(ret != Z_OK)
                raiseZlibError(ret);

        }

        ~GzDeflater()
        {
            ::deflateEnd(&zs);
        }

        /// Begin new compression input stream.
        /// @param src Input buffer address
        /// @param len Input buffer length
        void setInput(void const * src, size_t len)
        {
            ::deflateReset(&zs);
            zs.next_in = (::Bytef *)src;
            zs.avail_in = len;
            isDone = false;
            isStarted = false;
            boost::crc_32_type crcComputer;
            crcComputer.process_bytes(src,len);
            fileCrc = crcComputer.checksum();
            fileSize = len;
            }

        /// Continue compression input stream after last input is
        /// exhausted.  That is, the input stream should be initiated
        /// with setInput(), and continueInput() should only be called
        /// if inputRemaining() is zero, and done() is not true.
        /// @param src Input buffer address
        /// @param len Input buffer length
//        void continueInput(void const * src, size_t len)
//        {
//            assert(!done() && inputRemaining() == 0);
//            zs.next_in = (::Bytef *)src;
//            zs.avail_in = len;
//        }

        /// Return true if decompression is complete.
        bool done() const
        {
            return isDone;
        }
        
        /// Return number of bytes left in current input stream.
        size_t inputRemaining() const
        {
            return zs.avail_in;
        }
        
        // Each member has the following structure:
        //  +---+---+---+---+---+---+---+---+---+---+
        //  |ID1|ID2|CM |FLG|     MTIME     |XFL|OS |
        //  +---+---+---+---+---+---+---+---+---+---+
        // From http://www.gzip.org/zlib/rfc-gzip.html
        bool writeHeader(void * dst, size_t dstLen) const
        {
            if(dstLen < 10)
                return false;
            ((unsigned char*)dst)[0] = (int)0x1f;
            ((unsigned char*)dst)[1] = (int)0x8b;
            ((unsigned char*)dst)[2] = Z_DEFLATED;
            ((unsigned char*)dst)[3] = 0;
            ((int*)dst)[1] = (uint32_t)0;
            ((unsigned char*)dst)[8] = 0;
            ((unsigned char*)dst)[9] = OS_CODE;
            return true;
        }

        bool writeFooter(void * dst, size_t dstLen) const
        {
            if(dstLen < 8)
                return false;
            ((uint32_t*)dst)[0] = fileCrc;
            ((uint32_t*)dst)[1] = fileSize;
            return true;
        }

        /// Deflate to a target character buffer, as much as possible.
        /// @param dst Destination buffer address
        /// @param dstLen Destination buffer length
        /// @return Number of output bytes generated
        size_t deflate(void * dst, size_t dstLen)
        {
            size_t headerBytes = 0;
            if(!isStarted)
            {
                if(!writeHeader(dst, dstLen))
                    return 0;
                dstLen -= 10;
                headerBytes += 10;
                dst = &(((char*)dst)[10]);
                isStarted = true;
            }

            zs.next_out = (::Bytef *)dst;
            zs.avail_out = dstLen;
            int ret = ::deflate(&zs, Z_FINISH);
            switch(ret)
            {
                case Z_STREAM_END:  
                case Z_OK:          
                    if(writeFooter((char*)dst + (dstLen - zs.avail_out), zs.avail_out))
                    {
                        isDone = true;
                        headerBytes+=8;
                    }
                    break;
                default:            raiseZlibError(ret);
            }
            return dstLen - zs.avail_out + headerBytes;
        }

        /// Deflate to a target Buffer, as much as possible.
        /// @param dst Destination Buffer
        /// @return Number of output bytes generated
        size_t deflate(Buffer & dst)
        {
            char * p = dst.position();
            size_t sz = deflate(p, dst.remaining());
            dst.position(p + sz);
            return sz;
        }

        /// Deflate to a target Buffer, resizing if necessary.
        /// @param dst Destination Buffer
        /// @param outputRatio Expected ratio of output size to input size
        /// @param growthFactor Minimum growth to use when resizing
        /// @return Number of output bytes generated
        size_t deflateAll(Buffer & dst, float outputRatio = 0.2f,
                          float growthFactor = 2.0f)
              {
            assert(growthFactor > 1.0f);

            size_t startSz = dst.consumed();
            size_t inLeft;
            while(!done() && (inLeft = inputRemaining()))
            {
                size_t outSz = size_t(inLeft * outputRatio);
                if(dst.remaining() < outSz)
                {
                    size_t newCap = std::max(
                        dst.consumed() + outSz,
                        size_t(dst.capacity() * growthFactor));
                    Buffer tmp(newCap);
                    dst.flip();
                    tmp.put(dst);
                    tmp.swap(dst);
                }
                deflate(dst);
            }
            
            return dst.consumed() - startSz;
        }

        /// Deflate to an output iterator.  Iterated type should
        /// support \c char assignment, and iterator should be able to
        /// accomodate all output data.
        /// @param out Output iterator for destination
        /// @return Output iterator position at end of output data
        template <class OutIt>
        OutIt deflateAll(OutIt out)
        {
            enum { BUF_SIZE = 16 << 10 };
            char outBuf[BUF_SIZE];

            while(size_t sz = deflate(outBuf, BUF_SIZE))
                out = std::copy(outBuf, outBuf + sz, out);

            return out;
        }
    };

    /// Inflate gzip or zlib data to an output iterator.  Iterated
    /// type should support \c char assignment, and iterator should be
    /// able to accomodate all output data.
    /// @param src Compressed input buffer address
    /// @param len Compressed input buffer length
    /// @param out Output iterator for destination
    /// @return Output iterator position at end of output data
    template <class OutIt>
    OutIt gzip(void const * src, size_t len, OutIt out)
    {
        GzDeflater gz;
        gz.setInput(src, len);
        return gz.deflateAll(out);
    }

}

#endif // WARP_GZIP_H
