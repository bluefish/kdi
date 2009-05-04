//---------------------------------------------------------- -*- Mode: C++ -*-
// Public domain implementation of Adler-32 checksum from
// http://en.wikipedia.org/wiki/Adler-32
//----------------------------------------------------------------------------

#ifndef WARP_ADLER_H
#define WARP_ADLER_H

#include <stdint.h>
#include <stddef.h>

namespace warp {

    class Adler32
    {
        uint32_t a, b;
        enum { MOD_ADLER = 65521 };

    public:
        Adler32() { reset(); }
        void reset() { a = 1; b = 0; }
        uint32_t get() const { return (b << 16) | a; }

        void update(void const * data, size_t len)
        {
            uint8_t const * p = reinterpret_cast<uint8_t const *>(data);
            while(len)
            {
                size_t tlen = len > 5552 ? 5552 : len;
                len -= tlen;
                for(uint8_t const * end = p + tlen; p != end; ++p)
                {
                    a += *p;
                    b += a;
                }
                a %= MOD_ADLER;
                b %= MOD_ADLER;
            }
        }
    };

    inline uint32_t adler32(void const * data, size_t len)
    {
        Adler32 x;
        x.update(data, len);
        return x.get();
    }

} // namespace warp

#endif // WARP_ADLER_H
