//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/main/cosmix/src/cc/warp/kfs/kfsfs.cc#14 $
//
// Created 2006/09/19
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
//
//----------------------------------------------------------------------------

#include <warp/kfs/kfsfs.h>
#include <warp/uri.h>
#include <ex/exception.h>

#include <sys/stat.h>
#include <cerrno>
#include <vector>

using namespace warp::kfs;
using namespace warp;
using namespace ex;
using namespace KFS;
using std::string;
using std::vector;


//----------------------------------------------------------------------------
// KfsFilesystem
//----------------------------------------------------------------------------
KfsFilesystem::KfsFilesystem(KfsClientPtr const & client) :
    client(client)
{
    if(!client)
        raise<ValueError>("null KfsClient");
}

bool KfsFilesystem::mkdir(string const & uri)
{
    string path = fs::path(uri);

    int r = client->Mkdir(path.c_str());
    if(r != 0)
    {
        if(r == -EEXIST)
        {
            // Already exists -- okay if it's a directory
            struct stat st;
            if(0 == client->Stat(path.c_str(), st) && S_ISDIR(st.st_mode))
                return false;
        }
        raise<IOError>("mkdir failed for '%s': %s", uri, getStdError(-r));
    }
    return true;
}

bool KfsFilesystem::remove(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = client->Stat(path.c_str(), st);
    if(r == 0 && S_ISDIR(st.st_mode))
    {
        r = client->Rmdir(path.c_str());
        if(r != 0 && -r != ENOENT)
            raise<IOError>("rmdir failed for '%s': %s", uri, getStdError(-r));
        return r == 0;
    }
    else
    {
        r = client->Remove(path.c_str());
        if(r != 0 && -r != ENOENT)
            raise<IOError>("remove failed for '%s': %s", uri, getStdError(-r));
        return r == 0;
    }
}

void KfsFilesystem::rename(string const & sourceUri,
                           string const & targetUri,
                           bool overwrite)
{
    // Make sure we're on the same filesystem
    if(Filesystem::get(targetUri).get() != this)
        raise<RuntimeError>("cannot rename across filesystems: '%s' to '%s'",
                            sourceUri, targetUri);

    // Get paths
    string sourcePath = fs::path(sourceUri);
    string targetPath = fs::path(targetUri);

    // Make sure source exists
    struct stat st;
    int r = client->Stat(sourcePath.c_str(), st);
    if(r != 0)
        raise<IOError>("stat '%s' failed: %s", sourceUri, getStdError(-r));

    // Do rename
    r = client->Rename(sourcePath.c_str(), targetPath.c_str(), overwrite);
    if(r != 0)
        raise<IOError>("rename '%s' to '%s' failed: %s",sourceUri, targetUri,
                       getStdError(-r));
}

size_t KfsFilesystem::filesize(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = client->Stat(path.c_str(), st);
    if(r != 0)
        raise<IOError>("filesize failed for '%s': %s", uri, getStdError(-r));

    if(!S_ISREG(st.st_mode))
        raise<IOError>("filesize called on non-file '%s'", uri);

    return st.st_size;
}

bool KfsFilesystem::exists(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = client->Stat(path.c_str(), st);
    if(r != 0 && -r != ENOENT)
        raise<IOError>("exists failed for '%s': %s", uri, getStdError(-r));

    return r == 0;
}

bool KfsFilesystem::isDirectory(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = client->Stat(path.c_str(), st);
    if(r != 0 && -r != ENOENT)
        raise<IOError>("isDirectory failed for '%s': %s", uri,
                       getStdError(-r));

    return (r == 0 && S_ISDIR(st.st_mode));
}

bool KfsFilesystem::isFile(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = client->Stat(path.c_str(), st);
    if(r != 0 && -r != ENOENT)
        raise<IOError>("isFile failed for '%s': %s", uri, getStdError(-r));

    return (r == 0 && S_ISREG(st.st_mode));
}

bool KfsFilesystem::isEmpty(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = client->Stat(path.c_str(), st);
    if(r != 0)
        raise<IOError>("isEmpty failed for '%s': %s", uri, getStdError(-r));

    if(S_ISREG(st.st_mode))
    {
        return st.st_size == 0;
    }
    else if(S_ISDIR(st.st_mode))
    {
        vector<string> dir;
        r = client->Readdir(path.c_str(), dir);
        if(r != 0)
            raise<IOError>("read dir failed for '%s': %s", uri,
                           getStdError(-r));

        for(vector<string>::const_iterator i = dir.begin();
            i != dir.end(); ++i)
        {
            if(*i != "." && *i != "..")
                return false;
        }
        return true;
    }
    else
    {
        raise<IOError>("isEmpty called on non-file, non-directory '%s'", uri);
    }
}

double KfsFilesystem::modificationTime(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = client->Stat(path.c_str(), st);
    if(r != 0)
        raise<IOError>("modificationTime failed for '%s': %s", uri,
                       getStdError(-r));

    return double(st.st_mtime) + double(st.st_mtim.tv_nsec) * 1e-9;
}

double KfsFilesystem::accessTime(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = client->Stat(path.c_str(), st);
    if(r != 0)
        raise<IOError>("accessTime failed for '%s': %s", uri, getStdError(-r));

    return double(st.st_atime) + double(st.st_atim.tv_nsec) * 1e-9;
}

double KfsFilesystem::creationTime(string const & uri)
{
    string path = fs::path(uri);

    struct stat st;
    int r = client->Stat(path.c_str(), st);
    if(r != 0)
        raise<IOError>("creationTime failed for '%s': %s", uri,
                       getStdError(-r));

    return double(st.st_ctime) + double(st.st_ctim.tv_nsec) * 1e-9;
}

KfsClientPtr KfsFilesystem::getClient(std::string const & uri)
{
    KfsClientPtr client;

    StringRange host("localhost");
    StringRange port("20000");

    if(char * env = getenv("KFS_METASERVER"))
    {
        UriAuthority auth(env);
        if(auth.host)
            host = auth.host;
        if(auth.port)
            port = auth.port;
    }

    UriAuthority auth(Uri(uri).authority);
    if(auth.host)
        host = auth.host;
    if(auth.port)
        port = auth.port;

    int nport;
    if(parseInt(nport, port))
        client = getKfsClientFactory()->GetClient(host.toString(), nport);

    if(!client)
        raise<RuntimeError>("couldn't get KFS client: %s:%s",
                            host, port);

    return client;
}


//----------------------------------------------------------------------------
// Registration
//----------------------------------------------------------------------------
namespace
{
    FsPtr getKfsFs(string const & uri)
    {
        KfsClientPtr client = KfsFilesystem::getClient(uri);
        static FsPtr fs(new KfsFilesystem(client));
        return fs;
    }
}

#include <warp/init.h>
WARP_DEFINE_INIT(warp_kfs_kfsfs)
{
    Filesystem::registerScheme("kfs", &getKfsFs);
}
