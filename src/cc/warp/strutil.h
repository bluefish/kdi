//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/strutil.h#1 $
//
// Created 2006/01/10
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_STRUTIL_H
#define WARP_STRUTIL_H

#include "ex/exception.h"
#include <boost/range.hpp>
#include <warp/charmap.h>
#include <string>
#include <algorithm>
#include <stdint.h>
#include <assert.h>
#include <string.h>

namespace warp
{
    /// Type representing a range of constant character data.
    typedef boost::iterator_range<char const *> str_data_t;

    /// Wrap a std::string object.  NB: you're asking for a segfault
    /// if you try to wrap a temporary/immediate string, so don't do
    /// that.
    inline str_data_t wrap(std::string const & s)
    {
        char const * p = s.c_str();
        return str_data_t(p, p + s.size());
    }

    /// Construct a std::string object from a str_data_t.
    inline std::string str(str_data_t const & s)
    {
        return std::string(s.begin(), s.end());
    }

    /// Append a str_data_t range to a string.
    inline std::string & operator+=(std::string & a, str_data_t const & b)
    {
        a.append(b.begin(), b.end());
        return a;
    }

    /// Do lexicographical comparison of strings interpreted as
    /// unsigned bytes.
    inline bool string_compare(char const * a0, char const * a1,
                               char const * b0, char const * b1)
    {
        ptrdiff_t aLen = a1 - a0;
        ptrdiff_t bLen = b1 - b0;
        assert(aLen >= 0 && bLen >= 0);
        if(aLen < bLen)
            return memcmp(a0, b0, aLen) <= 0;
        else
            return memcmp(a0, b0, bLen) < 0;
    }

    /// Do lexicographical comparison of strings interpreted as
    /// unsigned bytes.
    inline bool string_compare(str_data_t const & a, str_data_t const & b)
    {
        return string_compare(a.begin(), a.end(),
                              b.begin(), b.end());
    }

    /// Do three-way lexicographical comparison of strings interpreted
    /// as unsigned bytes.
    inline int string_compare_3way(char const * a0, char const * a1,
                                   char const * b0, char const * b1)
    {
        ptrdiff_t aLen = a1 - a0;
        ptrdiff_t bLen = b1 - b0;
        assert(aLen >= 0 && bLen >= 0);
        if(int cmp = memcmp(a0, b0, (aLen < bLen ? aLen : bLen)))
            return cmp;
        else
            return aLen - bLen;
    }

    /// Do three-way lexicographical comparison of strings interpreted
    /// as unsigned bytes.
    inline int string_compare_3way(str_data_t const & a, str_data_t const & b)
    {
        return string_compare_3way(a.begin(), a.end(),
                                   b.begin(), b.end());
    }


    //------------------------------------------------------------------------
    // replace_inplace
    //------------------------------------------------------------------------
    void replace_inplace(std::string & s,
                         std::string const & match,
                         std::string const & repl);

    
    //------------------------------------------------------------------------
    // replace
    //------------------------------------------------------------------------
    inline std::string replace(std::string const & s,
                               std::string const & match,
                               std::string const & repl)
    {
        std::string r(s);
        replace_inplace(r, match, repl);
        return r;
    }

    /// Lowercase a string inplace
    inline std::string & lower_inplace(std::string & s)
    {
        std::transform(s.begin(), s.end(), s.begin(), CharMap::toLower());
        return s;
    }

    /// Return a lowercased copy of a string
    inline std::string lower(std::string const & s)
    {
        std::string r;
        std::transform(s.begin(), s.end(), std::back_inserter(r),
                       CharMap::toLower());
        return r;
    }

    /// Get a size as a pretty size string with a suffix.  For
    /// example, sizeString(1<<20) is "1M".
    std::string sizeString(int64_t sz, int64_t suffixFactor=1024);

    /// Parse a string as a size.  String may contain suffixes k/K,
    /// m/M, or g/G.
    size_t parseSize(std::string const & sizeString);

    /// Return a copy of source string with HTML tags stripped out.
    /// If \c insertSpaces is true, a space will be inserted where
    /// tags used to seperate text sections.  Otherwise tags are
    /// simply removed, which may cause some words to run together.
    std::string stripHtml(str_data_t const & src, bool insertSpaces);

    /// Get string representation of 4-byte typecode
    std::string typeString(uint32_t typecode);

    /// Get a string representation of a memory range, such that it is
    /// a valid string literal in C.  Special characters are escaped
    /// and non-printable characters are represented as C string
    /// escape codes.  It \c quote is true, the returned string will
    /// include surrounding double quotes.
    std::string reprString(char const * begin, char const * end,
                           bool quote=true);

    /// Get a string representation of a memory range, such that it is
    /// a valid string literal in C.  Special characters are escaped
    /// and non-printable characters are represented as C string
    /// escape codes.  It \c quote is true, the returned string will
    /// include surrounding double quotes.
    inline std::string reprString(void const * ptr, size_t len,
                                  bool quote=true)
    {
        char const * p = reinterpret_cast<char const *>(ptr);
        return reprString(p, p + len, quote);
    }

    /// Get a string representation of a memory range, such that it is
    /// a valid string literal in C.  Special characters are escaped
    /// and non-printable characters are represented as C string
    /// escape codes.  It \c quote is true, the returned string will
    /// include surrounding double quotes.
    inline std::string reprString(str_data_t const & s, bool quote=true)
    {
        return reprString(s.begin(), s.end(), quote);
    }

    /// Convert a byte range to a hex string.  Address referenced by
    /// \c out pointer must hold 2*(begin-end) bytes.
    void hexString(uint8_t const * begin, uint8_t const * end, char * out);

    /// Convert a byte range to a hex string.  New string is allocated
    /// and returned.
    std::string hexString(uint8_t const * begin, uint8_t const * end);

    /// Convert a byte range to a hex string.  New string is allocated
    /// and returned.
    inline std::string hexString(void const * ptr, size_t len)
    {
        uint8_t const * p = reinterpret_cast<uint8_t const *>(ptr);
        return hexString(p, p + len);
    }

    /// Get hex representation of an object.
    template <class T>
    std::string hexString(T const & x)
    {
        return hexString(&x, sizeof(x));
    }

    /// Dump a region of memory as a hex string, separated with spaces
    /// at aligned word boundaries.
    std::string memDump(void const * ptr, size_t len,
                        size_t wordSize = sizeof(int));

    /// Dump an object as a hex string, separated with spaces at
    /// aligned word boundaries.
    template <class T>
    std::string memDump(T const & x) { return memDump(&x, sizeof(x)); }


    /// Transform given string in place.  The transform lowercases all
    /// alphabetic characters and replaces all runs of non-alphabetic
    /// characters with a single space.  The normalized string will
    /// always begin and end with alphabetic characters, unless it is
    /// empty.
    void alphaNormalize(std::string & x);

    /// Skip space and tab characters.  Returns iterator past last
    /// space.  Assumes null-terminated string.
    template <class It>
    It skipSpace(It begin)
    {
        while(*begin && (*begin == ' ' || *begin == '\t'))
            ++begin;
        return begin;
    }

    /// Skip space and tab characters.  Returns iterator past last
    /// space.
    template <class It>
    It skipSpace(It begin, It end)
    {
        while(begin != end && (*begin == ' ' || *begin == '\t'))
            ++begin;
        return begin;
    }

    /// Skip backwards over trailing space and tab characters.
    /// Returns iterator pointing to first trailing space character.
    template <class It>
    It skipTrailingSpace(It begin, It end)
    {
        while(begin != end && (*(end-1) == ' ' || *(end-1) == '\t'))
            --end;
        return end;
    }

    /// Skip non-space and tab characters.  Returns iterator past last
    /// non-space.  Assumes null-terminated string.
    template <class It>
    It skipNonSpace(It begin)
    {
        while(*begin && *begin != ' ' && *begin != '\t')
            ++begin;
        return begin;
    }

    /// Skip non-space and tab characters.  Returns iterator past last
    /// non-space.
    template <class It>
    It skipNonSpace(It begin, It end)
    {
        while(begin != end && *begin != ' ' && *begin != '\t')
            ++begin;
        return begin;
    }

    /// Skip to newline character.  Returns iterator past last space.
    /// Assumes null-terminated string.
    template <class It>
    It skipToEndOfLine(It begin)
    {
        while(*begin && *begin != '\n')
            ++begin;
        return begin;
    }

    /// Skip to newline character.  Returns iterator past last space.
    template <class It>
    It skipToEndOfLine(It begin, It end)
    {
        while(begin != end && *begin != '\n')
            ++begin;
        return begin;
    }

    /// Convert a pair of hex characters to their corresponding byte
    /// value.
    /// @return Decoded byte value, or -1 if encoding is invalid.
    inline int dehex(char hi, char lo)
    {
        int x;
        switch(hi)
        {
            case '0':           x = 0x00; break;
            case '1':           x = 0x10; break;
            case '2':           x = 0x20; break;
            case '3':           x = 0x30; break;
            case '4':           x = 0x40; break;
            case '5':           x = 0x50; break;
            case '6':           x = 0x60; break;
            case '7':           x = 0x70; break;
            case '8':           x = 0x80; break;
            case '9':           x = 0x90; break;
            case 'a': case 'A': x = 0xa0; break;
            case 'b': case 'B': x = 0xb0; break;
            case 'c': case 'C': x = 0xc0; break;
            case 'd': case 'D': x = 0xd0; break;
            case 'e': case 'E': x = 0xe0; break;
            case 'f': case 'F': x = 0xf0; break;
            default:            return -1;
        }
        switch(lo)
        {
            case '0':           x |= 0x00; break;
            case '1':           x |= 0x01; break;
            case '2':           x |= 0x02; break;
            case '3':           x |= 0x03; break;
            case '4':           x |= 0x04; break;
            case '5':           x |= 0x05; break;
            case '6':           x |= 0x06; break;
            case '7':           x |= 0x07; break;
            case '8':           x |= 0x08; break;
            case '9':           x |= 0x09; break;
            case 'a': case 'A': x |= 0x0a; break;
            case 'b': case 'B': x |= 0x0b; break;
            case 'c': case 'C': x |= 0x0c; break;
            case 'd': case 'D': x |= 0x0d; break;
            case 'e': case 'E': x |= 0x0e; break;
            case 'f': case 'F': x |= 0x0f; break;
            default:            return -1;
        }
        return x;
    }
    
    /// Encode a UTF-32 character into UTF-8 and append it to a
    /// string.
    inline void encodeUtf8(uint32_t utf32Char, std::string & dst)
    {
        if(utf32Char < 0x0000007f)
        {
            dst += char(utf32Char);
        }
        else if(utf32Char < 0x000007ff)
        {
            dst += char( 0xc0 | (utf32Char >> 6));
            dst += char( 0x80 | (utf32Char & 0x3f));
        }
        else if(utf32Char < 0x0000ffff)
        {
            dst += char( 0xe0 | (utf32Char >> 12));
            dst += char( 0x80 | ((utf32Char >> 6) & 0x3f));
            dst += char( 0x80 | (utf32Char & 0x3f));
        }
        else if(utf32Char < 0x001fffff)
        {
            dst += char( 0xf0 | (utf32Char >> 18));
            dst += char( 0x80 | ((utf32Char >> 12) & 0x3f));
            dst += char( 0x80 | ((utf32Char >> 6) & 0x3f));
            dst += char( 0x80 | (utf32Char & 0x3f));
        }
        else if(utf32Char < 0x03ffffff)
        {
            dst += char( 0xf8 | (utf32Char >> 24));
            dst += char( 0x80 | ((utf32Char >> 18) & 0x3f));
            dst += char( 0x80 | ((utf32Char >> 12) & 0x3f));
            dst += char( 0x80 | ((utf32Char >> 6) & 0x3f));
            dst += char( 0x80 | (utf32Char & 0x3f));
        }
        else if(utf32Char < 0x7fffffff)
        {
            dst += char( 0xfc | (utf32Char >> 30));
            dst += char( 0x80 | ((utf32Char >> 24) & 0x3f));
            dst += char( 0x80 | ((utf32Char >> 18) & 0x3f));
            dst += char( 0x80 | ((utf32Char >> 12) & 0x3f));
            dst += char( 0x80 | ((utf32Char >> 6) & 0x3f));
            dst += char( 0x80 | (utf32Char & 0x3f));
        }
        else
        {
            using namespace ex;
            raise<ValueError>("invalid Unicode character: U+%08X",
                              utf32Char);
        }
    }

    /// Decode a single C-style escape sequence.  The range
    /// [begin,end) should contain the escape sequence, with begin
    /// pointing the first character after the backslash.  The decoded
    /// sequence is appended to a string and the pointer to the end of
    /// the input sequence is returned.
    template <class Iter>
    Iter decodeEscapeSequence(Iter begin, Iter end, std::string & dst)
    {
        using namespace ex;

        typename std::iterator_traits<Iter>::difference_type len =
            end - begin;
        if(len < 1)
            raise<ValueError>("empty escape sequence");

        switch(*begin)
        {
            case 'a':  dst += '\a'; return ++begin;
            case 't':  dst += '\t'; return ++begin;
            case 'n':  dst += '\n'; return ++begin;
            case 'f':  dst += '\f'; return ++begin;
            case 'r':  dst += '\r'; return ++begin;
            case 'v':  dst += '\v'; return ++begin;
            case '\\': dst += '\\'; return ++begin;

            case 'x':
            {
                int x;
                if(len < 3 || (x = dehex(begin[1], begin[2])) == -1)
                    raise<ValueError>("invalid hex escape sequence: \\%s",
                                      std::string(begin, std::min(begin+3, end)));
                dst += char(x);
                return begin + 3;
            }

            case 'u':
            {
                int a, b;
                if(len < 5 ||
                   (a = dehex(begin[1], begin[2])) == -1 ||
                   (b = dehex(begin[3], begin[4])) == -1)
                    raise<ValueError>("invalid Unicode escape sequence: \\%s",
                                      std::string(begin, std::min(begin+5, end)));
                uint32_t u = (uint32_t(a) << 8) | uint32_t(b);
                encodeUtf8(u, dst);
                return begin + 5;
            }

            case 'U':
            {
                int a, b, c, d;
                if(len < 9 ||
                   (a = dehex(begin[1], begin[2])) == -1 ||
                   (b = dehex(begin[3], begin[4])) == -1 ||
                   (c = dehex(begin[5], begin[6])) == -1 ||
                   (d = dehex(begin[7], begin[8])) == -1)
                    raise<ValueError>("invalid Unicode escape sequence: \\%s",
                                      std::string(begin, std::min(begin+9, end)));
                uint32_t u = ( (uint32_t(a) << 24) | (uint32_t(b) << 16) |
                               (uint32_t(c) << 8) | uint32_t(d) );
                encodeUtf8(u, dst);
                return begin + 9;
            }

            default:
                dst += *begin; return ++begin;
        }
    }


    //------------------------------------------------------------------------
    // parseInt
    //------------------------------------------------------------------------
    /// Convert a string to an integer
    template <class Int>
    bool parseInt(Int & r, char const * begin, char const * end)
    {
        char const * p = begin;
        
        Int x = 0;
        bool neg = false;

        if(p != end && *p == '-')
        {
            if(!std::numeric_limits<Int>::is_signed)
                return false;
            neg = true;
            ++p;
        }
        if(p == end)
            return false;
        do
        {
            if(*p >= '0' && *p <= '9')
                x = x * 10 + *p - '0';
            else
                return false;
        } while(++p != end);

        if(neg)
            r = -x;
        else
            r = x;
        return true;
    }

    /// Convert a string to an integer
    template <class Int>
    bool parseInt(Int & r, str_data_t const & s)
    {
        return parseInt(r, s.begin(), s.end());
    }

    /// Convert a string to an integer
    template <class Int>
    Int parseInt(str_data_t const & s)
    {
        Int x;
        if(parseInt<Int>(x, s))
            return x;
        else
        {
            using namespace ex;
            raise<ValueError>("invalid integer: %s", s);
        }
    }


    //------------------------------------------------------------------------
    // parseFloat
    //------------------------------------------------------------------------
    /// Convert a string to a float
    inline bool parseFloat(float & f, char const * begin, char const * end)
    {
        ptrdiff_t len = end - begin;
        if(len <= 0)
            return false;
        
        char buf[len+1];        // GCC extension
        memcpy(buf, begin, len);
        buf[len] = 0;

        char * stop;
        f = strtof(buf, &stop);
        return stop == &buf[len];
    }

    /// Convert a string to a float
    inline bool parseFloat(float & f, str_data_t const & s)
    {
        return parseFloat(f, s.begin(), s.end());
    }

    /// Convert a string to a float
    inline float parseFloat(str_data_t const & s)
    {
        float f;
        if(!parseFloat(f, s))
        {
            using namespace ex;
            raise<ValueError>("invalid float: %s", s);
        }
        return f;
    }

    //------------------------------------------------------------------------
    // PseudoNumericLt
    //------------------------------------------------------------------------
    /// Order strings in a pseudo-numerical fashion.  Strings that can
    /// be interpreted as integers are ordered numerically.
    /// Non-integer strings are ordered lexicographically.  Numeric
    /// strings are ordered before non-numeric strings.
    struct PseudoNumericLt
    {
        bool operator()(std::string const & a, std::string const & b)
        {
            char const * a0 = a.c_str();
            char const * a1 = a0 + a.size();
            char const * b0 = b.c_str();
            char const * b1 = b0 + b.size();
            int aVal, bVal;

            if(parseInt(aVal, a0, a1))
            {
                // A is a numeric string
                if(parseInt(bVal, b0, b1))
                    // So is B, order by value
                    return aVal < bVal;
                else
                    // B is not, A goes first
                    return true;
            }
            else if(parseInt(bVal, b0, b1))
                // A is not numeric but B is, B goes first
                return false;
            else
                // Neither is numeric, order lexicographically
                return a < b;
        }
    };
}


#endif // WARP_STRUTIL_H
