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

#include <warp/uri.h>
#include <warp/util.h>
#include <warp/strutil.h>
#include <warp/vstring.h>
#include <warp/charmap.h>
#include <algorithm>

using namespace warp;

namespace
{
    template <class It>
    It basename(It begin, It end)
    {
        // Scan back to last slash
        while(end != begin && *(end-1) != '/')
            --end;

        return end;
    }

    /// Resolve an individual backtrack path segment ("..") at the end
    /// of the range.  Returns the new end of the segment.  The given
    /// range must have a contain a trailing backtrack segment (a
    /// trailing ".." followed by an optional "/").  This routine very
    /// much depends on the invariants maintained by its only caller,
    /// normalize_dot_segments().  This is not meant to be a general
    /// purpose function.
    template <class It>
    It pop_segment(It begin, It end)
    {
        It p = end;

        // Skip trailing "/" if present
        assert(p != begin);
        if(*(p-1) == '/')
            --p;

        // Skip trailing ".." (should always be present)
        assert(p != begin && *(p-1) == '.');
        --p;
        assert(p != begin && *(p-1) == '.');
        --p;

        // If tail is all there is, don't pop it
        if(p == begin)
            return end;

        // Skip leading "/" in backtrack segment
        assert(*(p-1) == '/');
        --p;

        // Find beginning of last segment, counting dots 
        size_t dots = 0;
        for(; p != begin && *(p-1) != '/'; --p)
        {
            if(*(p-1) == '.')
                // Found a dot
                ++dots;
            else
                // This is not a backtrack segment
                dots = 3;
        }

        if(dots == 2)
            // Last is a backtrack segment -- don't pop
            return end;
        else if(dots == 0)
            // Last is a root segment -- discard pop, keep root
            return p+1;
        else
            // Last is a normal segment -- pop back to its parent
            return p;
    }

    /// Do dot normalization in a Unix-style path.  That is, "."
    /// references the current directory and ".." is a parent
    /// reference.  All "." references are eliminated and ".."
    /// segments are resolved as much as possible.
    template <class It>
    It normalize_dot_segments(It begin, It end)
    {
        // dots has four states:
        //   dots <= 0 : nothing scanned in segment
        //   dots == 1 : 1 byte into segment, and it's a dot
        //   dots == 2 : 2 bytes into segment, and both are dots
        //   dots >= 3 : segment is >2 bytes or contains non-dots
        int dots = 0;
        
        It o = begin;
        for(It i = begin; i != end; ++i)
        {
            switch(*i)
            {
                case '/':
                    if(dots >= 3)
                        *o++ = *i;
                    else if(dots == 1)
                        --o;
                    else if(dots == 2)
                    {
                        *o++ = *i;
                        o = pop_segment(begin, o);
                    }
                    else if(o == begin)
                        *o++ = *i;

                    dots = 0;
                    break;

                case '.':
                    ++dots;
                    *o++ = *i;
                    break;
                    
                default:
                    dots = 3;
                    *o++ = *i;
                    break;
            }
        }

        if(dots == 2)
            o = pop_segment(begin, o);
        else if(dots == 1)
            --o;

        return o;
    }

    /// Describe every char as permitted or barred in a URI scheme,
    /// as specified in RFC 3986 (Section 3.1).
    ///
    /// (SchemeChar()[c] & FIRSTCHAR) iff c can start a scheme string;
    /// (SchemeChar()[c] & OTHERCHAR) iff c is usable for subsequent chars;
    /// (SchemeChar()[c] == INVALID) if c can never appear in a scheme.
    class SchemeChar
    {
        char m[256];
    public:
        static const char INVALID = 0x00;
        static const char FIRSTCHAR = 0x01;
        static const char OTHERCHAR = 0x02;
        SchemeChar()
        {
            for (int i = 0; i < 256; ++i)
                m[i] = INVALID;

            const char letters[] =
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
            for (size_t i = 0; i < sizeof(letters)-1; ++i)
                m[(uint8_t)letters[i]] = (FIRSTCHAR | OTHERCHAR);

            const char other[] = "0123456789.+-";
            for (size_t i = 0; i < sizeof(other)-1; ++i)
                m[(uint8_t)other[i]] = OTHERCHAR;
        }
        inline bool validFirst(char x) const { return m[(uint8_t)x] & FIRSTCHAR; }
        inline bool validSubsequent(char x) const { return m[(uint8_t)x] & OTHERCHAR; }
    };
    const SchemeChar schemeChar;

    /// Scan a character range for the terminating ':' of a URI scheme.  A
    /// valid URI scheme is of the form "[A-Za-z][A-Za-z0-9.+-]*:".  If
    /// the scheme component is valid, a pointer to the terminating ':' is
    /// returned.  Otherwise, the end pointer is returned.
    char const * scanUriScheme(char const * begin, char const * end)
    {
        // First char must be in [A-Za-z]
        if(begin == end || !schemeChar.validFirst(*begin))
            return end;
    
        for(char const * p = begin + 1; p != end; ++p)
        {
            // Got terminator?
            if(*p == ':')
                return p;
    
            // Subsequent chars must be in [A-Za-z0-9.+-]
            if(!schemeChar.validSubsequent(*p))
                break;
        }
    
        // Invalid scheme
        return end;
    }

}


//----------------------------------------------------------------------------
// Uri
//----------------------------------------------------------------------------
str_data_t Uri::popScheme() const
{
    if(!schemeDefined())
        return *this;
    char const * p = std::find(scheme.begin(), scheme.end(), '+');
    // There is an explicit scheme, so *p is either '+' or ':' at this
    // point.  Either pop everything up to and including the '+', or
    // drop the whole scheme (up to and including the ':').
    return str_data_t(++p, end());
}

str_data_t Uri::topScheme() const
{
    return str_data_t(
        scheme.begin(),
        std::find(scheme.begin(), scheme.end(), '+')
        );
}


//----------------------------------------------------------------------------
// Configurable implementation
//----------------------------------------------------------------------------
void Uri::resolve(Uri const & ref, VString & out, int allow) const
{
    bool allowBaseAuthority = true;
    bool allowBasePath = true;
    bool allowBaseQuery = true;

#undef ANY
#undef SAME
#undef ALLOW

#define ANY(X,Y)  X ## _ANY_ ## Y
#define SAME(X,Y) X ## _SAME_ ## Y
#define ALLOW(X,Y,same) ((allow & ANY(X,Y) & ~SAME(X,Y)) || ((allow & SAME(X,Y)) && (same)))

    // Scheme
    if(ref.schemeDefined())
    {
        out += ref.scheme;
        out += ':';

        allowBaseAuthority = ALLOW(AUTH,  SCHEME, ref.scheme == this->scheme);
        allowBasePath      = ALLOW(PATH,  SCHEME, ref.scheme == this->scheme);
        allowBaseQuery     = ALLOW(QUERY, SCHEME, ref.scheme == this->scheme);
    }
    else if(this->schemeDefined())
    {
        out += this->scheme;
        out += ':';
    }

    // Authority
    bool outHasAuth = false;
    if(ref.authorityDefined())
    {
        out += '/'; out += '/';
        out += ref.authority;
        outHasAuth = true;

        allowBasePath  = allowBasePath  && ALLOW(PATH,  AUTHORITY, ref.authority == this->authority);
        allowBaseQuery = allowBaseQuery && ALLOW(QUERY, AUTHORITY, ref.authority == this->authority);
    }
    else if(allowBaseAuthority && this->authorityDefined())
    {
        out += '/'; out += '/';
        out += this->authority;
        outHasAuth = true;
    }

    // Path
    size_t pathBegin = out.size();
    if(allowBasePath)
    {
        if(!ref.path)
            out += this->path;
        else if(*ref.path.begin() == '/')
            out += ref.path;
        else
        {
            char const * b = basename(this->path.begin(), this->path.end());
            if(this->path.begin() != b)
                out.append(this->path.begin(), b);
            else if(outHasAuth)
                out += '/';
            out += ref.path;
        }
    }
    else
        out += ref.path;
    
    // Normalize path
    out.erase(normalize_dot_segments(out.begin() + pathBegin, out.end()),
              out.end());

    // Is the base query still allowed?
    if(allowBaseQuery && ref.path)
    {
        allowBaseQuery = 
            ALLOW(QUERY, PATH, (out.empty() && !this->path) || this->path ==
                  str_data_t(&out[0]+pathBegin, &out[0]+out.size()) );
    }

    // Query
    if(ref.queryDefined())
    {
        out += '?';
        if(allowBaseQuery && (allow & QUERY_APPEND) && this->queryDefined())
        {
            out += this->query;
            if(ref.query)
                out += '&';
        }
        out += ref.query;
    }
    else if(allowBaseQuery && this->queryDefined())
    {
        out += '?';
        out += this->query;
    }

    // Fragment
    if(ref.fragmentDefined())
    {
        out += '#';
        out += ref.fragment;
    }
}

//----------------------------------------------------------------------------
// RFC 3986 reference implementation
//----------------------------------------------------------------------------
#if 0
void Uri::resolve(Uri const & ref, VString & out, bool strict) const
{
    str_data_t scheme;
    str_data_t auth;
    str_data_t path;
    str_data_t relPath;
    str_data_t query;

    bool schemeDef = false;
    bool authDef = false;
    bool queryDef = false;

    bool removePathDots = true;

    // Select output parts based on what's defined in the reference URI
    if(ref.schemeDefined() && (strict || ref.scheme != this->scheme))
    {
        // Ref has a scheme defined (or a different scheme, in the
        // case of non-strict resolution) so use the whole ref URI.
        scheme = ref.scheme;
        auth = ref.authority;
        path = ref.path;
        query = ref.query;

        schemeDef = true;
        authDef = ref.authorityDefined();
        queryDef = ref.queryDefined();
    }
    else
    {
        // Ref has no scheme (or the same scheme in the case of
        // non-strict resolution).  Use the base scheme.
        scheme = this->scheme;
        schemeDef = this->schemeDefined() || ref.schemeDefined();

        if(ref.authorityDefined())
        {
            // Ref has an authority -- use the rest of the ref URI.
            auth = ref.authority;
            path = ref.path;
            query = ref.query;

            authDef = true;
            queryDef = ref.queryDefined();
        }
        else
        {
            // Ref has no authority -- use the base authority.
            auth = this->authority;
            authDef = this->authorityDefined();
            
            if(!ref.path)
            {
                // Ref has no path, use base path straight up
                path = this->path;
                removePathDots = false;

                if(ref.queryDefined())
                {
                    // Ref has a query -- use it
                    query = ref.query;
                    queryDef = ref.queryDefined();
                }
                else
                {
                    // Ref has no query -- use base query
                    query = this->query;
                    queryDef = this->queryDefined();
                }
            }
            else
            {
                // Ref has a path
                if(*ref.path.begin() == '/')
                {
                    // Ref path is absolute -- use it and ignore base path
                    path = ref.path;
                }
                else
                {
                    // Ref path is relative -- compose with base path
                    path = this->path;
                    relPath = ref.path;
                }
                // Since ref has a path, always use ref's query (this
                // is standard, but it could be useful to do this
                // another way)
                query = ref.query;
                queryDef = ref.queryDefined();
            }
        }
    }

    // Compose scheme
    if(schemeDef)
    {
        out += scheme;
        out += ':';
    }

    // Compose authority
    if(authDef)
    {
        out += '/';
        out += '/';
        out += auth;
    }
    
    // Remember start of path section
    size_t pStart = out.size();

    // Compose path
    if(relPath)
    {
        // Find last slash in base path
        char const * p = path.end();
        while(p != path.begin() && p[-1] != '/')
            --p;

        // Append base up to and including last slash
        out.append(path.begin(), p);
        
        // Append relative path
        out += relPath;
    }
    else
    {
        // Just have one path
        out += path;
    }

    // Maybe normalize path dots?
    if(removePathDots)
    {
        // Normalize path dots inplace
        out.erase(normalize_dot_segments(out.begin() + pStart, out.end()),
                  out.end());
    }

    // Compose query
    if(queryDef)
    {
        out += '?';
        out += query;
    }

    // Compose fragment (always use ref fragment)
    if(ref.fragmentDefined())
    {
        out += '#';
        out += ref.fragment;
    }
}
#endif // reference implementation of Uri::resolve()


void Uri::parse()
{
    char const * p = begin();
    char const * pEnd = end();
    char const * pp;

    pp = scanUriScheme(p, pEnd);
    if(pp != pEnd)
    {
        scheme = str_data_t(p, pp);
        p = pp + 1;
    }
    else
        scheme = str_data_t(p, p);
    
    // Parse authority
    if(pEnd - p >= 2 && p[0] == '/' && p[1] == '/')
    {
        for(pp = p+2; pp != pEnd; ++pp)
        {
            if(*pp == '/' || *pp == '?' || *pp == '#')
                break;
        }
        authority = str_data_t(p+2, pp);
        p = pp;
    }
    else
        authority = str_data_t(p,p);

    // Parse path
    for(pp = p; pp != pEnd; ++pp)
    {
        if(*pp == '?' || *pp == '#')
            break;
    }
    path = str_data_t(p, pp);
    p = pp;

    // Parse query
    if(p != pEnd && *p == '?')
    {
        pp = std::find(p+1, pEnd, '#');
        query = str_data_t(p+1, pp);
        p = pp;
    }
    else
        query = str_data_t(p,p);

    // Parse fragment
    if(p != pEnd && *p == '#')
        fragment = str_data_t(p+1, pEnd);
    else
        fragment = str_data_t(p,p);
}


//----------------------------------------------------------------------------
// UriAuthority
//----------------------------------------------------------------------------
void UriAuthority::parse()
{
    char const * p = begin();
    char const * pEnd = end();
    char const * pp;

    for(pp = p; pp != pEnd; ++pp)
    {
        if(*pp == ':' || *pp == '@')
            break;
    }

    if(pp == pEnd)
    {
        // no ':' or '@' -- it's all host
        user = str_data_t(p,p);
        pass = str_data_t(p,p);
        host = str_data_t(p,pEnd);
        port = str_data_t(pEnd,pEnd);
    }
    else if(*pp == '@')
    {
        // found '@' first -- user@(...)
        user = str_data_t(p, pp);
        pass = str_data_t(pp,pp);
        p = pp+1;
        pp = std::find(p, pEnd, ':');
        host = str_data_t(p, pp);
        if(pp == pEnd)
            // no ':' -- user@host
            port = str_data_t(pEnd, pEnd);
        else
            // found ':' -- user@host:port
            port = str_data_t(pp+1, pEnd);
    }
    else // *pp == ':'
    {
        // found ':' first -- either user:(pass@...) or host:(port)
        char const * ppp = std::find(pp+1, pEnd, '@');
        if(ppp != pEnd)
        {
            // also found '@' -- user:pass@(...)
            user = str_data_t(p, pp);
            pass = str_data_t(pp+1, ppp);
            p = ppp + 1;
            pp = std::find(p, pEnd, ':');
            host = str_data_t(p, pp);
            if(pp == pEnd)
                // no second ':' -- user:pass@host
                port = str_data_t(pEnd, pEnd);
            else
                // found second ':' -- user:pass@host:port
                port = str_data_t(pp+1, pEnd);
        }
        else
        {
            // no '@' -- host:(port)
            user = str_data_t(p, p);
            pass = str_data_t(p, p);
            host = str_data_t(p, pp);
            port = str_data_t(pp+1, pEnd);
        }
    }
}


//----------------------------------------------------------------------------
// UriQuery
//----------------------------------------------------------------------------
void UriQuery::parseFirst()
{
    value = str_data_t(begin(), begin());
    parseNext();
}

void UriQuery::parseNext()
{
    char const * p = value.end();
    char const * pEnd = end();

    // Skip trailer from last attribute
    while(p != pEnd && *p == '&')
        ++p;

    // Search for key
    char const * pp;
    for(pp = p; pp != pEnd; ++pp)
    {
        if(*pp == '&' || *pp == '=')
            break;
    }
    key = str_data_t(p, pp);

    // Search for value if we terminated the key on '='
    if(pp != pEnd && *pp == '=')
        value = str_data_t(pp+1, std::find(pp+1, pEnd, '&'));
    else
        value = str_data_t(pp, pp);
}

//----------------------------------------------------------------------------
// Utils
//----------------------------------------------------------------------------
namespace
{
    class CharSet
    {
        char d[256];

    public:
        CharSet()
        {
            clear();
        }

        CharSet(char const * s)
        {
            clear(); set(s);
        }
        
        void set(char const * s)
        {
            while(*s)
                d[uint8_t(*s++)] = 1;
        }

        void clear()
        {
            memset(d, 0, sizeof(d));
        }

        bool operator()(char c) const
        {
            return d[uint8_t(c)];
        }
    };

    CharSet const UNRESERVED_SET("abcdefghijklmnopqrstuvwxyz"
                                 "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                 "0123456789._~-");
};

namespace
{
    char const HEX[] = "0123456789abcdef";

    template <class It1, class It2>
    It2 encode(It1 begin, It1 end, It2 out,
               bool spaceToPlus, char const * safe)
    {
        CharSet isSafe(UNRESERVED_SET);
        isSafe.set(safe);

        for(; begin != end; ++begin)
        {
            if(isSafe(*begin))
                *out++ = *begin;
            else if(*begin == ' ' && spaceToPlus)
                *out++ = '+';
            else
            {
                *out++ = '%';
                *out++ = HEX[(*begin >> 4) & 0x0f];
                *out++ = HEX[*begin & 0x0f];
            }
        }

        return out;
    }

}

std::string warp::uriEncode(strref_t s, bool spaceToPlus, char const * safe)
{
    std::string r;
    encode(s.begin(), s.end(),
           std::back_inserter(r),
           spaceToPlus, safe);
    return r;
}

namespace
{
    inline void recode(VString & out, VString & tmp,
                       char const * begin, char const * end,
                       bool convertPlus, char const * safe)
    {
        if(begin != end)
        {
            tmp.resize(end - begin);

            VString::iterator tEnd = 
                uriDecode(begin, end,
                          tmp.begin(), tmp.end(),
                          convertPlus);
                
            encode(tmp.begin(), tEnd,
                   std::back_inserter(out),
                   convertPlus, safe);
        }
    }

    inline void recode(VString & out, VString & tmp,
                       str_data_t const & in,
                       bool convertPlus, char const * safe)
    {
        recode(out, tmp, in.begin(), in.end(),
               convertPlus, safe);
    }

    enum {
        SCHEME_HTTP = 0,
        SCHEME_HTTPS,
        SCHEME_FTP,
        SCHEME_GOPHER,

        N_SCHEMES,
        SCHEME_OTHER,
    };

    char const * const DEFAULT_PORT[N_SCHEMES] =
    {
        "80",
        "443",
        "21",
        "70",
    };
}

std::string warp::uriNormalize(strref_t uri)
{
    VString out;
    VString tmp;
    Uri u(uri);

    // Output lowercased scheme.  Also, check to see if we recognize
    // the scheme for later port normalization.
    int scheme = SCHEME_OTHER;
    if(u.schemeDefined())
    {
        // Output lower-cased scheme
        std::transform(
            u.scheme.begin(), u.scheme.end(),
            std::back_inserter(out), CharMap::toLower());

        // See if we know this scheme
        switch(out[0])
        {
            case 'h':
                if(out == "http")
                    scheme = SCHEME_HTTP;
                else if(out == "https")
                    scheme = SCHEME_HTTPS;
                break;

            case 'f':
                if(out == "ftp")
                    scheme = SCHEME_FTP;
                break;

            case 'g':
                if(out == "gopher")
                    scheme = SCHEME_GOPHER;
                break;
        }

        out += ':';
    }

    // Output normalized authority.  The host is lowercased and
    // recoded with escape characters as appropriate.  The user:pass
    // section are recoded.  The port is only included if it is not
    // the default port of a known scheme.
    if(u.authorityDefined())
    {
        out += '/'; out += '/';

        UriAuthority a(u.authority);
        if(a.user)
        {
            recode(out, tmp, a.user.begin(), a.pass.end(),
                   false, "!$&'()*+,;=" ":");
            out += '@';
        }
        size_t n = out.size();
        recode(out, tmp, a.host, false, "!$&'()*+,;=");
        std::transform(out.begin()+n, out.end(),
                       out.begin()+n, CharMap::toLower());
        if(a.port)
        {
            if(scheme == SCHEME_OTHER ||
               a.port != DEFAULT_PORT[scheme])
            {
                out += ':';
                out += a.port;
            }
        }
    }

    // Output normalized path.  The path is recoded and extra slashes
    // and dot segments are resolved.
    {
        size_t n = out.size();
        recode(out, tmp, u.path, false, "!$&'()*+,;=" ":@/");
        out.erase(
            normalize_dot_segments(out.begin() + n, out.end()),
            out.end()
            );
    }

    // Output normalized query.  Key/value pairs are recoded.  Pairs
    // with an empty key are omitted.  Empty values omit the '='
    // separator.  If there is no query string, the '?' is omitted.
    // This transform isn't really safe.
    if(u.query)
    {
        out += '?';
        size_t n = out.size();
        
        bool first = true;
        for(UriQuery q(u.query); q.hasAttr(); q.next())
        {
            if(!q.key)
                continue;
            
            if(first)
                first = false;
            else
                out += '&';

            recode(out, tmp, q.key, true, "!$'()*+,;" ":@/?");

            if(q.value)
            {
                out += '=';
                recode(out, tmp, q.value, true, "!$'()*+,;" ":@/?");
            }
        }

        if(out.size() == n)
            out.pop_back();
    }

    // Output normalized fragment.  The fragment value is recoded.  If
    // there is no fragment value, the '#' is omitted.  This is also
    // unsafe.
    if(u.fragment)
    {
        out += '#';
        recode(out, tmp, u.fragment, false, "!$&'()*+,;=" ":@/?");
    }

    return std::string(out.begin(), out.end());
}

std::string warp::uriResolve(strref_t uriBase, strref_t uriRef, int allow)
{
    Uri base(uriBase);
    Uri ref(uriRef);
    VString out;
    base.resolve(ref, out, allow);
    return std::string(out.begin(), out.end());
}

std::string warp::uriGetParameter(strref_t uri, strref_t encodedKey)
{
    UriQuery q(Uri(uri).query);
    if(q.find(encodedKey))
        return str(q.value);
    else
        return std::string();
}

std::string warp::uriSetParameter(strref_t uri, strref_t encodedKey,
                                  strref_t encodedValue)
{
    // Parse URI
    Uri u(uri);

    // Scan parameters
    std::string outUri(u.begin(), u.path.end());
    bool first = true;
    bool found = false;
    for(UriQuery q(u.query); q.hasAttr(); q.next())
    {
        // Check to see if the parameter name matches
        if(q.key == encodedKey)
        {
            // Only keep first instance of the parameter
            if(found) continue;
            found = true;

            // Replace value
            outUri += (first ? '?' : '&');
            first = false;
            outUri.append(encodedKey.begin(), encodedKey.end());
            if(encodedValue)
            {
                outUri += '=';
                outUri.append(encodedValue.begin(), encodedValue.end());
            }
        }
        else
        {
            // Copy existing parameter
            outUri += (first ? '?' : '&');
            first = false;
            outUri.append(q.key.begin(), q.value.end());
        }
    }

    // Append parameter if we didn't already replace it
    if(!found)
    {
        outUri += (first ? '?' : '&');
        outUri.append(encodedKey.begin(), encodedKey.end());
        if(encodedValue)
        {
            outUri += '=';
            outUri.append(encodedValue.begin(), encodedValue.end());
        }
    }

    // Append rest of URI (i.e. fragment)
    outUri.append(u.query.end(), u.end());
        
    // Done
    return outUri;
}

std::string warp::uriEraseParameter(strref_t uri, strref_t encodedKey)
{
    // Parse URI
    Uri u(uri);

    // Scan parameters
    std::string outUri(u.begin(), u.path.end());
    bool first = true;
    for(UriQuery q(u.query); q.hasAttr(); q.next())
    {
        // Skip parameter
        if(q.key == encodedKey)
            continue;

        // Otherwise output parameter
        outUri += (first ? '?' : '&');
        first = false;
        outUri.append(q.key.begin(), q.value.end());
    }

    // Append rest of URI (i.e. fragment)
    outUri.append(u.query.end(), u.end());
        
    // Done
    return outUri;
}

std::string warp::uriPopScheme(strref_t uri)
{
    Uri u(uri);
    return str(u.popScheme());
}

std::string warp::uriTopScheme(strref_t uri)
{
    Uri u(uri);
    return str(u.topScheme());
}

std::string warp::uriPushScheme(strref_t uri, strref_t scheme)
{
    // If the scheme is empty, just return the original URI
    if(scheme.empty())
        return str(uri);

    // Build new URI.  Start with the scheme.
    std::string r(scheme.begin(), scheme.end());

    // Append an appropriate separator -- if the URI has a scheme, use
    // a '+', otherwise our scheme is the new scheme, so use ':'
    Uri u(uri);
    if(u.schemeDefined())
        r += '+';
    else
        r += ':';

    // Append the rest of the URI
    r.append(uri.begin(), uri.end());
    return r;
}
