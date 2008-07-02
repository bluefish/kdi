//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/bufferedfile.cc#1 $
//
// Created 2006/05/04
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "bufferedfile.h"
#include "ex/exception.h"
#include <algorithm>
#include <assert.h>

using namespace warp;
using namespace ex;


//----------------------------------------------------------------------------
// BufferedFile -- private inline
//----------------------------------------------------------------------------
size_t BufferedFile::fillBuffer()
{
    assert(mode == MODE_READ);
    assert(pos == fp->tell());

    buf.compact();
    size_t readSz = buf.read(fp);
    pos += readSz;
    buf.flip();

    return readSz;
}

void BufferedFile::spillBuffer()
{
    assert(mode == MODE_WRITE);

    buf.flip();
    if(!buf.empty())
    {
        assert(pos == fp->tell());

        size_t writeSz = buf.write(fp);
        pos += writeSz;

        if(!buf.empty())
            raise<IOError>("couldn't write buffer");
    }
    buf.clear();
}

void BufferedFile::requireMode(Mode m)
{
    if(mode != m)
    {
        if(mode == MODE_WRITE)
            spillBuffer();
        else
            buf.clear();

        mode = m;
        if(mode == MODE_READ)
            buf.flip();
    }
}


//----------------------------------------------------------------------------
// BufferedFile
//----------------------------------------------------------------------------
BufferedFile::BufferedFile(FilePtr const & fp, size_t bufSz) :
    buf(bufSz), fp(fp), pos(0), mode(MODE_READ)
{
    EX_CHECK_NULL(fp);
    pos = fp->tell();
    buf.flip();
}

BufferedFile::~BufferedFile()
{
    if(mode == MODE_WRITE)
        spillBuffer();
}

size_t BufferedFile::read(void * dst, size_t elemSz, size_t nElem)
{
    requireMode(MODE_READ);
    if(elemSz > buf.capacity())
        raise<IOError>("elemSz exceeds buffer capacity: sz=%s cap=%s",
                       elemSz, buf.capacity());

    size_t readSz = elemSz * nElem;
    char * dstp = reinterpret_cast<char *>(dst);
    for(;;)
    {
        size_t bufSz = buf.remaining();
        if(readSz <= bufSz)
        {
            // Complete read
            buf.get(dstp, readSz);
            return nElem;
        }
        else if(bufSz >= elemSz)
        {
            // Read partial
            size_t partSz = bufSz - (bufSz % elemSz);
            buf.get(dstp, partSz);
            dstp += partSz;
            readSz -= partSz;
        }

        // Fill buffer from file
        fillBuffer();

        if(buf.remaining() < elemSz)
        {
            // Didn't even get one element
            return nElem - (readSz / elemSz);
        }
    }
}

size_t BufferedFile::readline(char * dst, size_t sz, char delim)
{
    requireMode(MODE_READ);

    // Claim space for null termination at dst[sz]
    if(!sz)
        return 0;
    --sz;

    // Read up to EOF, delim, or sz - 1 bytes
    size_t len = 0;

    while(len < sz)
    {
        // See what's left in the buffer
        size_t rem = buf.remaining();
        if(!rem)
        {
            // Nothing left -- refill
            fillBuffer();
            
            if(!rem)
            {
                // End of file
                dst[len] = 0;
                return len;
            }
        }

        // Copy until we find delim or run out of either buffer
        size_t endSz = std::min(sz, len + rem);
        while(len < endSz)
        {
            char c = buf.get();
            dst[len++] = c;
            if(c == delim)
            {
                // Found delim
                dst[len] = 0;
                return len;
            }
        }
    }

    // End of output buffer
    dst[len] = 0;
    return len;
}

size_t BufferedFile::write(void const * src, size_t elemSz, size_t nElem)
{
    requireMode(MODE_WRITE);
    if(elemSz > buf.capacity())
        raise<IOError>("elemSz exceeds buffer capacity: sz=%s cap=%s",
                       elemSz, buf.capacity());

    size_t writeSz = elemSz * nElem;
    char const * srcp = reinterpret_cast<char const *>(src);
    for(;;)
    {
        size_t bufSz = buf.remaining();
        if(writeSz <= bufSz)
        {
            // Complete write
            buf.put(srcp, writeSz);
            return nElem;
        }
        else if(bufSz >= elemSz)
        {
            // Write partial
            size_t partSz = bufSz - (bufSz % elemSz);
            buf.put(srcp, partSz);
            srcp += partSz;
            writeSz -= partSz;
        }

        // Write buffer to file
        spillBuffer();
    }
}

void BufferedFile::flush()
{
    spillBuffer();
    fp->flush();
}

void BufferedFile::close()
{
    if(mode == MODE_WRITE)
        spillBuffer();
    fp->close();
}

off_t BufferedFile::tell() const
{
    if(mode == MODE_READ)
    {
        return pos - buf.remaining();
    }
    else
    {
        assert(mode == MODE_WRITE);
        return pos + buf.consumed();
    }
}

void BufferedFile::seek(off_t offset, int whence)
{
    if(mode == MODE_READ)
    {
        // Try to seek within buffer
        if(whence == SEEK_CUR)
        {
            off_t offMin = buf.begin() - buf.position();
            off_t offMax = buf.limit() - buf.position();
            if(offset >= offMin && offset <= offMax)
            {
                buf.position(buf.begin() + (offset - offMin));
                return;
            }
        }
        else if(whence == SEEK_SET)
        {
            off_t offMax = pos;
            off_t offMin = offMax - (buf.limit() - buf.begin());
            if(offset >= offMin && offset <= offMax)
            {
                buf.position(buf.begin() + (offset - offMin));
                return;
            }
        }

        // Seek is outside of buffered region
        fp->seek(offset, whence);
        pos = fp->tell();
        buf.clear();
        buf.flip();
    }
    else
    {
        // Don't try anything fancy with writes
        assert(mode == MODE_WRITE);
        spillBuffer();
        fp->seek(offset, whence);
        pos = fp->tell();
    }
}

std::string BufferedFile::getName() const
{
    return fp->getName();
}

bool BufferedFile::skipTo(char const * pattern, size_t patternLen,
                          size_t maxRead)
{
    requireMode(MODE_READ);
    
    if(patternLen > buf.capacity())
        raise<IOError>("pattern length exceed buffer capacity: len=%s cap=%s",
                       patternLen, buf.capacity());

    // Search forward while we can possibly find the full pattern
    while(maxRead >= patternLen)
    {
        // Get the size of the data to scan from the buffer
        size_t scanSz = std::min(buf.remaining(), maxRead);

        // Search for a match if the buffer could contain one
        if(scanSz >= patternLen)
        {
            char * pos = buf.position();
            char * lim = pos + scanSz;
            char * res = std::search(pos, lim, pattern, pattern + patternLen);

            if(res != lim)
            {
                // Found a match
                buf.position(res);
                return true;
            }
            else
            {
                // No match, seek back so we don't miss occurances
                // straddling buffer boundaries
                buf.position(lim - patternLen + 1);
                maxRead -= buf.position() - pos;
            }
        }

        // Toss consumed data and refill buffer
        size_t readSz = fillBuffer();

        // Check for EOF
        if(!readSz)
            return false;
    }

    // No more matches possible
    return false;
}

char BufferedFile::get(off_t p)
{
    requireMode(MODE_READ);

    // 'pos' is the file position at the end of the buffer
    ptrdiff_t bufSz = buf.limit() - buf.begin();
    off_t distanceFromEnd = pos - p;
    ptrdiff_t bufPos = bufSz - distanceFromEnd;

    if(bufPos >= 0 && bufPos < bufSz)
    {
        // Requested position is in buffered region
        return *(buf.begin() + bufPos);
    }
    else
    {
        // Outside of buffered region
        seek(p);
        char c;
        if(!read(&c, 1))
            raise<IOError>("read invalid file position: %s#%d",
                           getName(), p);
        return c;
    }
}
