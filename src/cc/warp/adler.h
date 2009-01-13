/*
 * Public domain implementation of Adler-32 checksum from http://en.wikipedia.org/wiki/Adler-32
 */

#include <stdint.h>
#include <stddef.h>

uint32_t adler(const uint8_t *data, size_t len);
