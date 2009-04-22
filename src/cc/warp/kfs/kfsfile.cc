//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/main/cosmix/src/cc/warp/kfs/kfsfile.cc#13 $
//
// Created 2006/06/27
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
//
//----------------------------------------------------------------------------

#include <warp/kfs/kfsfile.h>
#include <warp/kfs/kfsfs.h>
#include <warp/fs.h>
#include <warp/uri.h>
#include <ex/exception.h>
#include <cassert>

using namespace warp::kfs;
using namespace warp;
using namespace ex;
using KFS::KfsClientPtr;

KfsFile::KfsFile(KfsClientPtr const & client, std::string const & uri, int flags) :
    client(client), fd(-1), fn(uri)
{
    if(!client)
        raise<ValueError>("null KfsClient");

    std::string path = fs::path(uri);
    int numReplicas = 3;

    Uri u(wrap(uri));
    for(UriQuery q(u.query); q.hasAttr(); q.next())
    {
        if(q.key == "rep")
        {
            numReplicas = parseInt<int>(q.value);
            if(numReplicas < 1)
                raise<RuntimeError>("KFS replication count must be positive");
            break;
        }
    }

    fd = client->Open(path.c_str(), flags, numReplicas);
    if(fd < 0)
        raise<IOError>("failed to open '%s' (path=%s)", fn, path);
}

KfsFile::~KfsFile()
{
    close();
}

void KfsFile::flush()
{
    if(fd < 0)
        raise<IOError>("flush() on closed file: %s", fn);

    client->Sync(fd);
}

void KfsFile::close()
{
    if(fd >= 0)
    {
        client->Close(fd);
        fd = -1;
    }
}

off_t KfsFile::tell() const
{
    if(fd < 0)
        raise<IOError>("tell() on closed file: %s", fn);

    return client->Tell(fd);
}

void KfsFile::seek(off_t offset, int whence)
{
    if(fd < 0)
        raise<IOError>("seek() on closed file: %s", fn);

    client->Seek(fd, offset, whence);
}

size_t KfsFile::readline(char *dst, size_t sz, char delim)
{
    if(fd < 0)
        raise<IOError>("readline() on closed file: %s", fn);

    if(!sz)
        return 0;
    --sz;

    size_t len = 0;
    while(len < sz)
    {
        // kfs client does read-ahead and buffered I/O. So, it is ok
        // to read char by char.
        char c;
        if(client->Read(fd, &c, 1) != 1)
            break;

        dst[len++] = c;

        if(c == delim)
            break;
    }

    // return length of line read
    dst[len] = 0;
    return len;
}

//
// We are expected to return the # of elements that were successfully read.
//
size_t KfsFile::read(void * dstv, size_t elemSz, size_t nElem)
{
    if(fd < 0)
        raise<IOError>("read() on closed file: %s", fn);
    if(!elemSz)
        raise<ValueError>("need positive element size");

    // Retry reads while we can get more data
    char * dst = reinterpret_cast<char *>(dstv);
    size_t bytesLeft = elemSz * nElem;
    while(bytesLeft)
    {
        // Read into whatever buffer space we have left
        ssize_t readSz = client->Read(fd, dst, bytesLeft);

        if(readSz > 0)
        {
            // Managed to read something
            bytesLeft -= readSz;
            dst += readSz;
        }
        else if(readSz < 0)
        {
            // Read error
            raise<IOError>("read() %d of %d bytes from '%s' failed: %s",
                           bytesLeft, elemSz * nElem, fn,
                           getStdError(-readSz));
        }
        else
        {
            // Didn't read anything this time.  Don't retry or we'll
            // likely get stuck in a loop.
            break;
        }
    }

    // The File API is to read in indivisible units of elemSz.  KFS
    // provides a byte-level interface.  Make up the difference here.
    if(bytesLeft > 0)
    {
        // Had some partial read leftover, compute number of complete
        // elements read
        size_t bytesRead = nElem * elemSz - bytesLeft;
        size_t nRead = bytesRead / elemSz;

        // Check for partial element write remainder
        off_t rem = bytesRead - elemSz * nRead;
        if(rem)
        {
            // Seek back to the end of the last full element.
            seek(-rem, SEEK_CUR);
        }

        // Return number of complete elements read
        return nRead;
    }
    else
    {
        // Managed to read everything
        return nElem;
    }
}


//
// We are expected to return the # of elements that were successfully written.
//
size_t KfsFile::write(void const * srcv, size_t elemSz, size_t nElem)
{
    if(fd < 0)
        raise<IOError>("write() on closed file: %s", fn);
    if(!elemSz)
        raise<ValueError>("need positive element size");

    // Retry writes while we can make forward progress
    char const * src = reinterpret_cast<char const *>(srcv);
    size_t bytesLeft = elemSz * nElem;
    while(bytesLeft)
    {
        // Write whatever we have left
        ssize_t writeSz = client->Write(fd, src, bytesLeft);

        if(writeSz > 0)
        {
            // Managed to write something
            bytesLeft -= writeSz;
            src += writeSz;
        }
        else if(writeSz < 0)
        {
            // Write error
            raise<IOError>("write() %d of %d bytes to '%s' failed: %s",
                           bytesLeft, elemSz * nElem, fn,
                           getStdError(-writeSz));
        }
        else
        {
            // Didn't write anything this time.  Don't retry or we'll
            // likely get stuck in a loop.
            break;
        }
    }

    // The File API is to write in indivisible units of elemSz.  KFS
    // provides a byte-level interface.  Make up the difference here.
    if(bytesLeft > 0)
    {
        // Had some partial write leftover, compute number of complete
        // elements written
        size_t bytesWritten = nElem * elemSz - bytesLeft;
        size_t nWritten = bytesWritten / elemSz;

        // Check for partial element write remainder
        off_t rem = bytesWritten - elemSz * nWritten;
        if(rem)
        {
            // Seek back to the end of the last full element.
            seek(-rem, SEEK_CUR);
        }

        // Return number of complete elements written
        return nWritten;
    }
    else
    {
        // Managed to write everything
        return nElem;
    }
}

namespace
{
    FilePtr openKfs(string const & uri, int flags)
    {
        KfsClientPtr client = KfsFilesystem::getClient(uri);
        FilePtr fp(new KfsFile(client, uri, flags));
        return fp;
    }
}

#include <warp/init.h>
WARP_DEFINE_INIT(warp_kfs_kfsfile)
{
    File::registerScheme("kfs", &openKfs);
}
