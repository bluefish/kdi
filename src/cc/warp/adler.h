//---------------------------------------------------------- -*- Mode: C++ -*-
// Public domain implementation of Adler-32 checksum from
// http://en.wikipedia.org/wiki/Adler-32
//----------------------------------------------------------------------------

#include <stdint.h>
#include <stddef.h>

namespace warp {

    uint32_t adler(void const * data, size_t len);

}
