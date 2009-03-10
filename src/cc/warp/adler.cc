//---------------------------------------------------------- -*- Mode: C++ -*-
// Public domain implementation of Adler-32 checksum from
// http://en.wikipedia.org/wiki/Adler-32
//----------------------------------------------------------------------------

#include <warp/adler.h>

#define MOD_ADLER 65521
 
/* data: Pointer to the data to be summed; len is in bytes */
uint32_t warp::adler(void const * data_, size_t len)
{
    uint8_t const * data = reinterpret_cast<uint8_t const *>(data_);
    uint32_t a = 1, b = 0;

    while (len > 0) 
    {
        size_t tlen = len > 5552 ? 5552 : len;
        len -= tlen;
        do 
        {
            a += *data++;
            b += a;
        } while (--tlen);
                                                                                
        a %= MOD_ADLER;
        b %= MOD_ADLER;
    }
    
    return (b << 16) | a;
}
