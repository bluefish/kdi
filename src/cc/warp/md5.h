//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2005 Josh Taylor (Kosmix Corporation)
// Created 2005-12-06
// 
// This file is part of the warp library.
// 
// The warp library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The warp library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

// Adapted from MD5 code at
//   http://userpages.umbc.edu/~mabzug1/cs/md5/md5.html
//
// Serious clean-up by Josh Taylor 2005-12-06

#ifndef WARP_MD5_H
#define WARP_MD5_H

// MD5.CC - source code for the C++/object oriented translation and 
//          modification of MD5.

// Translation and modification (c) 1995 by Mordechai T. Abzug 

// This translation/ modification is provided "as is," without express or 
// implied warranty of any kind.

// The translator/ modifier does not claim (1) that MD5 will do what you think 
// it does; (2) that this translation/ modification is accurate; or (3) that 
// this software is "merchantible."  (Language for this disclaimer partially 
// copied from the disclaimer below).

/* based on:

MD5.H - header file for MD5C.C
MDDRIVER.C - test driver for MD2, MD4 and MD5

Copyright (C) 1991-2, RSA Data Security, Inc. Created 1991. All
rights reserved.

License to copy and use this software is granted provided that it
is identified as the "RSA Data Security, Inc. MD5 Message-Digest
Algorithm" in all material mentioning or referencing this software
or this function.

License is also granted to make and use derivative works provided
that such works are identified as "derived from the RSA Data
Security, Inc. MD5 Message-Digest Algorithm" in all material
mentioning or referencing the derived work.

RSA Data Security, Inc. makes no representations concerning either
the merchantability of this software or the suitability of this
software for any particular purpose. It is provided "as is"
without express or implied warranty of any kind.

These notices must be retained in any copies of any part of this
documentation and/or software.

*/

#include <stdint.h>
#include <stddef.h>
#include <string>

namespace warp
{
    class Md5
    {
        uint64_t count;
        uint32_t state[4];
        uint8_t buffer[64];
        bool finalized;

        void init();
        void transform(uint8_t const * buffer);

    public:
        Md5() { init(); }

        // Update hash with more data.  If the hash has already been
        // finalized with digest(), the hash will be reinitialized.
        void update(void const * data, size_t len);

        // Finalize hash and export digest.  Digest size is 16 bytes.
        void digest(void * dst /* 16 bytes */);

        // Convenience digest function.
        static inline void digest(void * dst, void const * src, size_t len)
        {
            Md5 x;
            x.update(src, len);
            x.digest(dst);
        }
    };

    /// Compute a MD5 hash from a string and return the hash as a hex
    /// string.
    std::string md5HexDigest(std::string const & str);
}

#endif // WARP_MD5_H
