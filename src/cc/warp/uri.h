//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-08-13
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

#ifndef WARP_URI_H
#define WARP_URI_H

#include <warp/strutil.h>
#include <warp/strref.h>

namespace warp
{
    // Forward
    class VString;

    //------------------------------------------------------------------------
    // Uri
    //------------------------------------------------------------------------
    struct Uri : public str_data_t
    {
        typedef str_data_t super;

        str_data_t scheme;
        str_data_t authority;
        str_data_t path;
        str_data_t query;
        str_data_t fragment;

        Uri() {}
        Uri(strref_t s) : super(s) { parse(); }

        template <class It>
        Uri(It begin, It end) : super(begin, end) { parse(); }

        /// For composition schemes (e.g. foo+bar://...), pop the
        /// first scheme component and return the rest
        /// (e.g. bar://...).  If the scheme is undefined or only a
        /// single component, pop the entire scheme section and return
        /// a URI with an undefined scheme.
        str_data_t popScheme() const;

        /// For composition schemes (e.g. foo+bar://...), return the
        /// first scheme component (e.g. foo).  If the scheme is
        /// undefined or empty, the result is empty.
        str_data_t topScheme() const;

        /// Resolution liberties
        enum {
            STRICT = 0,         

            AUTH_SAME_SCHEME      = (1 << 0),
            AUTH_ANY_SCHEME       = (1 << 1)  | AUTH_SAME_SCHEME,

            PATH_SAME_SCHEME      = (1 << 2),
            PATH_ANY_SCHEME       = (1 << 3)  | PATH_SAME_SCHEME,
            PATH_SAME_AUTHORITY   = (1 << 4),
            PATH_ANY_AUTHORITY    = (1 << 5)  | PATH_SAME_AUTHORITY,

            QUERY_SAME_SCHEME     = (1 << 6),
            QUERY_ANY_SCHEME      = (1 << 7)  | QUERY_SAME_SCHEME,
            QUERY_SAME_AUTHORITY  = (1 << 8),
            QUERY_ANY_AUTHORITY   = (1 << 9)  | QUERY_SAME_AUTHORITY,
            QUERY_SAME_PATH       = (1 << 10),
            QUERY_ANY_PATH        = (1 << 11) | QUERY_SAME_PATH,
            QUERY_APPEND          = (1 << 12),

            SAME_SCHEME = ( AUTH_SAME_SCHEME  | PATH_SAME_SCHEME  |
                            QUERY_SAME_SCHEME ),
            ANY_SCHEME  = ( AUTH_ANY_SCHEME   | PATH_ANY_SCHEME   |
                            QUERY_ANY_SCHEME  ),

            SAME_AUTHORITY = ( PATH_SAME_AUTHORITY | QUERY_SAME_AUTHORITY ),
            ANY_AUTHORITY  = ( PATH_ANY_AUTHORITY  | QUERY_ANY_AUTHORITY  ),

            RFC_1630 = SAME_SCHEME,   // RFC 1630 notion of resolution
            RFC_3986 = STRICT,        // RFC 3986 notion of resolution
        };

        /// Resolve a URI reference relative to this as a base.  Store
        /// output string in \c out.  The \c allow field permits
        /// certain liberties in URI resolution:
        ///
        /// Use base authority unless any of the following are true:
        ///  - ref defines scheme          && !AUTH_SAME_SCHEME
        ///  - ref scheme is different     && !AUTH_ANY_SCHEME
        ///  - ref defines authority
        ///
        /// Merge base path unless any of the following are true:
        ///  - ref defines scheme          && !PATH_SAME_SCHEME
        ///  - ref scheme is different     && !PATH_ANY_SCHEME
        ///  - ref defines authority       && !PATH_SAME_AUTHORITY
        ///  - ref authority is different  && !PATH_ANY_AUTHORITY
        ///  - ref path is absolute
        ///
        /// In the event of a base path merge, the ref path
        /// appended to the dirname of the base path, and then dot
        /// elements in the path are collapsed.
        ///
        /// Merge base query unless any of the following are true:
        ///  - ref defines scheme          && !QUERY_SAME_SCHEME
        ///  - ref scheme is different     && !QUERY_ANY_SCHEME
        ///  - ref defines authority       && !QUERY_SAME_AUTHORITY
        ///  - ref authority is different  && !QUERY_ANY_AUTHORITY
        ///  - ref has non-empty path      && !QUERY_SAME_PATH
        ///  - resolved path is different  && !QUERY_ANY_PATH
        ///  - ref defines a query         && !QUERY_APPEND
        ///  - base query is empty
        ///
        /// In the event of a base query merge, the ref query is
        /// appended to the base query.  If the ref query is
        /// non-empty, the base and ref query will be separated by
        /// an ampersand.
        ///
        /// In all cases, X_ANY_Y implies X_SAME_Y is also allowed.
        ///
        /// The default is \c STRICT, which conforms with RFC 3986.
        void resolve(Uri const & ref, VString & out, int allow=STRICT) const;

        /// Return true if the scheme section was defined in the
        /// source string.  It may still be empty.
        bool schemeDefined() const
        {
            return (scheme.end() < authority.begin() &&
                    *scheme.end() == ':');
        }

        /// Return true if the authority section was defined in the
        /// source string.  It may still be empty.
        bool authorityDefined() const
        {
            char const * a = authority.begin();
            return (a - scheme.end() >= 2 &&
                    a[-1] == '/' &&
                    a[-2] == '/');
        }

        /// Return true if the query section was defined in the source
        /// string.  It may still be empty.
        bool queryDefined() const
        {
            return (path.end() < query.begin() &&
                    query.begin()[-1] == '?');
        }

        /// Return true if the fragment section was defined in the
        /// source string.  It may still be empty.
        bool fragmentDefined() const
        {
            return (query.end() < fragment.begin() &&
                    fragment.begin()[-1] == '#');
        }

    private:
        void parse();
    };


    //------------------------------------------------------------------------
    // UriAuthority
    //------------------------------------------------------------------------
    struct UriAuthority : public str_data_t
    {
        typedef str_data_t super;

        str_data_t user;
        str_data_t pass;
        str_data_t host;
        str_data_t port;

        UriAuthority() {}

        template <class Range>
        explicit UriAuthority(Range r) : super(r) { parse(); }

        template <class Iter>
        UriAuthority(Iter first, Iter last) : super(first, last) { parse(); }

    private:
        void parse();
    };


    //------------------------------------------------------------------------
    // UriQuery
    //------------------------------------------------------------------------
    struct UriQuery : public str_data_t
    {
        typedef str_data_t super;

        // Iterated attributes
        str_data_t key;
        str_data_t value;

        UriQuery() {}

        template <class Range>
        explicit UriQuery(Range r) : super(r) { parseFirst(); }

        template <class Iter>
        UriQuery(Iter first, Iter last) : super(first, last) { parseFirst(); }

        bool hasAttr() const { return key.begin() != end(); }

        bool next()
        {
            parseNext();
            return hasAttr();
        }

        bool reset()
        {
            parseFirst();
            return hasAttr();
        }

        /// Scan the parameter list for the first key matching the
        /// given name.  Return true iff a match is found.  The value
        /// will be in the \c value attribute, as usual.
        bool find(strref_t keyName)
        {
            parseFirst();
            while(hasAttr() && key != keyName)
                parseNext();
            return hasAttr();
        }
        
    private:
        void parseFirst();
        void parseNext();
    };


    /// Decode an escaped URI string.  Hex escape sequences are
    /// converted to the characters they encode.  Invalid hex
    /// encodings are ignored and skipped.  If \c plusToSpace is true,
    /// plus characters ('+') will be converted to spaces.  Since this
    /// is a non-increasing conversion, the conversion can happen in
    /// place if the src and dst memory regions are the same.
    /// @return Iterator pointing to end of output
    template <class It1, class It2>
    It2 uriDecode(It1 srcBegin, It1 srcEnd, It2 dstBegin, It2 dstEnd,
                  bool plusToSpace = false)
    {
        if(dstBegin == dstEnd)
            return dstEnd;

        It2 d = dstBegin;
        for(It1 s = srcBegin; s != srcEnd; ++s)
        {
            switch(*s)
            {
                case '+':
                    *d = (plusToSpace ? ' ' : '+');
                    break;

                case '%':
                    if(srcEnd - s >= 3)
                    {
                        // Decode hex sequence
                        int x = dehex(*(s+1), *(s+2));
                        s += 2;
                        if(x != -1)
                            // Keep decoded value
                            *d = char(x);
                        else
                            // Invalid encoding, ignore it
                            --d;
                    }
                    else
                    {
                        // Short hex encoding, ignore it
                        return d;
                    }
                    break;
                    
                default:
                    *d = *s;
                    break;
            }

            if(++d == dstEnd)
                break;
        }
        return d;
    }

    /// Decode an escaped URI string.  Hex escape sequences are
    /// converted to the characters they encode.  Invalid hex
    /// encodings are ignored and skipped.  If \c plusToSpace is true,
    /// plus characters ('+') will be converted to spaces.
    /// @return Decoded string
    inline std::string uriDecode(strref_t s, bool plusToSpace = false)
    {
        char buf[s.size()];
        char * end = uriDecode(s.begin(), s.end(),
                               buf, buf + sizeof(buf),
                               plusToSpace);
        return std::string(buf, end);
    }

    /// Encode a string for safe embedding in a URI.  Characters not
    /// in the URI unreserved set [a-zA-Z0-9._~-] or the user supplied
    /// safe set are replaced with escape sequences.  If \c
    /// spaceToPlus is true, spaces are escaped to '+'.  Otherwise
    /// they're escaped to "%20".  The default safe set is suitable
    /// for encoding components of the path and query sections of most
    /// URI schemes.
    std::string uriEncode(strref_t s, bool spaceToPlus = false,
                          char const * safe = "/'!*:(),");

    /// Return a normalized copy of the given URI.
    std::string uriNormalize(strref_t uri);

    /// Resolve a URI reference relative to the given base.  Return a
    /// copy of the resolves URI.
    /// @see Uri::resolve()
    std::string uriResolve(strref_t uriBase, strref_t uriRef,
                           int allow=Uri::STRICT);

    /// Get the value associated with the first occurence of a key in
    /// the given URI's query string.  If the key is not found, an
    /// empty value is returned.  Note that an empty return value does
    /// not necessarily mean that the key does not exist.  The key
    /// value is assumed to be URI encoded.  The returned value is a
    /// substring of the input URI, and so is URI encoded if the input
    /// URI is well-behaved.
    std::string uriGetParameter(strref_t uri, strref_t encodedKey);

    /// Construct a new URI with the given key/value pair set in the
    /// query string.  If the old URI already contains the key, the
    /// first occurence is replaced with the new value and subsequent
    /// occurences are erased.  Otherwise, the new key is appended to
    /// the query parameter list.  If the value is empty, the '=' is
    /// omitted after the key.  Both the key and value are assumed to
    /// be URI encoded.
    std::string uriSetParameter(strref_t uri, strref_t encodedKey,
                                strref_t encodedValue);

    /// Construct a new URI with all occurrences of a key stripped out
    /// of the query string.  The key name is assumed to be URI
    /// encoded.
    std::string uriEraseParameter(strref_t uri, strref_t encodedKey);

    /// Construct a new URI by popping the front scheme component.
    /// @see Uri::popScheme()
    std::string uriPopScheme(strref_t uri);

    /// Get the top scheme component of the URI.
    /// @see Uri::topScheme()
    std::string uriTopScheme(strref_t uri);

    /// Construct a new URI by pushing a new scheme component on the
    /// front of the URI.  If the URI already has a scheme, the new
    /// and old schemes will be separated by a plus (+).  The scheme
    /// is assumed to contain only valid scheme characters.
    std::string uriPushScheme(strref_t uri, strref_t scheme);
}

#endif // WARP_URI_H
