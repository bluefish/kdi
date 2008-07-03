//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-09-18
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

#ifndef WARP_FS_H
#define WARP_FS_H

#include <string>
#include <vector>
#include <stddef.h>
#include <boost/shared_ptr.hpp>

namespace warp
{
    class Filesystem;

    // Filesystem independent URI-path manipulation functions
    namespace fs
    {
        /// Resolve relative URI with respect to a base URI.  The base
        /// is assumed to be a directory, which is somewhat different
        /// from normal URI reference resolution.  If the given base
        /// URI's path does not end with a slash, a slash will be
        /// appended to it prior to URI resolution.
        std::string resolve(std::string const & baseUri,
                            std::string const & relUri);

        /// Get path component of a URI.
        std::string path(std::string const & uri);

        /// Replace path component of a URI.
        /// @return new URI with path replaced
        std::string replacePath(std::string const & origUri,
                                std::string const & replacementPath);

        /// Get basename from path component of a URI.  The basename
        /// includes everything after the last slash in the path.  For
        /// paths that end with a slash, this may be an empty string.
        /// If \c stripExt is true, strip extension from basename.
        std::string basename(std::string const & uri,
                             bool stripExt = false);

        /// Get dirname from path component of a URI.  The dirname
        /// includes everything before last slash in the path.  The
        /// last slash is not included.  Note that this function only
        /// returns a substring of the path and will strip the rest of
        /// the URI info.  If you want a parent directory, use
        /// resolve(uri, "..").
        std::string dirname(std::string const & uri);

        /// Get the extension from a path component of a URI.  The
        /// extension is the suffix after and including the last
        /// period in the basename() of the URI.  If the basename has
        /// no period or only a leading period (e.g. ".ext"), an empty
        /// string is returned.
        std::string extension(std::string const & uri);

        /// Change the extension on the path component of a URI.  The
        /// old extension (if any) is replaced by the new extension.
        /// The new extension should include a leading period if
        /// desired.  If the new extension is empty, the effect is to
        /// strip the extension from the path.  If the basename of the
        /// path is empty (e.g. path ends with a slash), then the
        /// original URI is returned unchanged.
        std::string changeExtension(std::string const & uri,
                                    std::string const & newExt);

        /// Create a directory and all necessary parent directories.
        /// If directory already exists, do nothing.
        void makedirs(std::string const & uri);

        /// Recursively remove a directory (or file) and all its
        /// contents.  If the given path does not exist, do nothing.
        /// This is a best effort and doesn't attempt to handle race
        /// conditions where other processes are populating the
        /// directory as it is being cleaned.
        void removedirs(std::string const & uri);

        /// Returns true iff the given URI has a rooted path.  That
        /// is, the first character in the path is a slash.
        bool isRooted(std::string const & uri);

        /// Returns true iff the given URI is a relative path that
        /// starts with one or more parent ("..") path segments.
        bool isBackpath(std::string const & uri);

#if 0
        /// @todo File globbing isn't quite right yet.  Needs proper
        /// relative path handling.  Also, it has a problem with
        /// appending trailing slashes to things it shouldn't when
        /// there are trailing parameters in the URI.  Basically, the
        /// desired behavior for URIs needs to be thought out before
        /// more work goes into this.  However, it's not high priority
        /// at the moment, so what's here is disabled and checked in
        /// as a work in progress.  (2007-05-01 Josh)

        /// Expand wildcards in a URI to a set of matching URIs for
        /// which exists() is true.
        std::vector<std::string> glob(std::string const & uri);

        /// Expand wildcards in a URI to a set of matching URIs for
        /// which exists() is true.  Expanded results are appended to
        /// result set.
        void globAppend(std::string const & uri, std::vector<std::string> & result);
#endif
    }

    // Free functions that will automatically dispatch to the
    // appropriate Filesystem implementation.
    //
    // Typical implementation:
    //
    // RType func(std::string const & uri, ...)
    // {
    //     return Filesystem::get(uri)->func(uri, ...);
    // }
    namespace fs
    {
        /// Create a directory.  Parent directory must already exist.
        /// If target already exists as a directory, don't complain.
        /// @return true if directory was created, false if directory
        /// already existed
        bool mkdir(std::string const & uri);

        /// Remove a file or directory.  If path is a directory, the
        /// directory must be empty.  If path does not exist, nothing
        /// is done.
        /// @return true if path existed before being removed.
        bool remove(std::string const & uri);

        /// Rename a file or directory.  Most implementations will
        /// require that the source and target URIs map to the same
        /// filesystem.  If \c overwrite is true, replace targetUri
        /// with sourceUri, even if it already exists.  Otherwise,
        /// only allow rename if target does not exist.  Note that
        /// this is a "best effort" attempt.  Not all filesystem
        /// implementations can guarantee atomicity.  Also, some
        /// filesystems may have more difficulty renaming directories.
        void rename(std::string const & sourceUri,
                    std::string const & targetUri,
                    bool overwrite = false);
    
        /// Get the size of a file in bytes.  Path must exist.
        size_t filesize(std::string const & uri);

        /// Return true if the path exists.
        bool exists(std::string const & uri);
        
        /// Return true if the path exists and is a directory.
        bool isDirectory(std::string const & uri);

        /// Return true if the path exists and is a regular file.
        bool isFile(std::string const & uri);

        /// Return true if the path points to an empty file or
        /// directory.  Path must exist.
        bool isEmpty(std::string const & uri);

        /// Return modification time of a file or directory, measured
        /// in seconds from the Epoch (00:00:00 UTC, January 1, 1970).
        double modificationTime(std::string const & uri);

        /// Return access time of a file or directory, measured in
        /// seconds from the Epoch (00:00:00 UTC, January 1, 1970).
        /// Not all implementations support this in a meaningful way.
        double accessTime(std::string const & uri);

        /// Return creation time of a file or directory, measured in
        /// seconds from the Epoch (00:00:00 UTC, January 1, 1970).
        /// Not all implementations support this in a meaningful way.
        double creationTime(std::string const & uri);
    }

    typedef boost::shared_ptr<Filesystem> FsPtr;
    typedef FsPtr (FsMapFunc)(std::string const & uri);
}


//----------------------------------------------------------------------------
// Filesystem
//----------------------------------------------------------------------------
class warp::Filesystem
{
public:
    virtual ~Filesystem() {}

    /// Create a directory.  Parent directory must already exist.  If
    /// target already exists as a directory, don't complain.
    /// @return true if directory was created, false if directory
    /// already existed
    virtual bool mkdir(std::string const & uri) = 0;

    /// Remove a file or directory.  If path is a directory, the
    /// directory must be empty.  If path does not exist, nothing is
    /// done.
    /// @return True if path existed before being removed.
    virtual bool remove(std::string const & uri) = 0;

    /// Rename a file or directory.  Most implementations will require that
    /// the source and target URIs map to the same filesystem.  If \c
    /// overwrite is true, replace targetUri with sourceUri, even if
    /// it already exists.  Otherwise, only allow rename if target
    /// does not exist.  Note that this is a "best effort" attempt.
    /// Not all filesystem implementations can guarantee atomicity.
    /// Also, some filesystems may have more difficulty renaming
    /// directories.
    virtual void rename(std::string const & sourceUri,
                        std::string const & targetUri,
                        bool overwrite) = 0;

    /// Get the size of a file in bytes.  Path must exist.
    virtual size_t filesize(std::string const & uri) = 0;

    /// Return true if the path exists.
    virtual bool exists(std::string const & uri) = 0;

    /// Return true if the path exists and is a directory.
    virtual bool isDirectory(std::string const & uri) = 0;

    /// Return true if the path exists and is a regular file.
    virtual bool isFile(std::string const & uri) = 0;

    /// Return true if the path points to an empty file or directory.
    /// Path must exist.
    virtual bool isEmpty(std::string const & uri) = 0;

    /// Return last modification time of a file or directory, measured
    /// in seconds from the Epoch (00:00:00 UTC, January 1, 1970).
    virtual double modificationTime(std::string const & uri) = 0;

    /// Return last access time of a file or directory, measured in
    /// seconds from the Epoch (00:00:00 UTC, January 1, 1970).  Not
    /// all implementations support this in a meaningful way.
    virtual double accessTime(std::string const & uri) = 0;

    /// Return creation time of a file or directory, measured in
    /// seconds from the Epoch (00:00:00 UTC, January 1, 1970).  Not
    /// all implementations support this in a meaningful way.
    virtual double creationTime(std::string const & uri) = 0;



    //----------------------------------------------------------------------
    // Registration and dispatch

    /// Get the Filesystem responsible for the given URI.
    static FsPtr get(std::string const & uri);

    /// Register a URI-to-Filesystem mapping for a given scheme.
    static void registerScheme(char const * scheme, FsMapFunc * func);
    
    /// Set a default scheme for URIs with no scheme otherwise
    /// specified.
    static void setDefaultScheme(char const * scheme);
};




#endif // WARP_FS_H
