#!/usr/bin/env python
#
# Copyright (C) 2005 Josh Taylor (Kosmix Corporation)
# Created 2005-11-29
# 
# This file is part of the util library.
# 
# The util library is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or any later
# version.
# 
# The util library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

#----------------------------------------------------------------------------
# Circular
#----------------------------------------------------------------------------
class Circular(object):
    """A node in a circular list."""
    def __init__(self):
        """Create single node circular list."""
        self.prev = self
        self.next = self
    def unlink(self):
        """Unlink this node from whatever list it may be in and make
        it a single element list."""
        self.next.prev = self.prev
        self.prev.next = self.next
        self.next = self
        self.prev = self
    def insertAfter(self, circ):
        """Insert this node after the given node."""
        if self is not circ:
            self.next.prev = self.prev
            self.prev.next = self.next
            self.prev = circ
            self.next = circ.next
            circ.next.prev = self
            circ.next = self


#----------------------------------------------------------------------------
# CacheItem
#----------------------------------------------------------------------------
class CacheItem(Circular):
    """An item in the LRU cache."""
    def __init__(self, key=None, data=None):
        Circular.__init__(self)
        self.key = key
        self.data = data


#----------------------------------------------------------------------------
# FileMapCache
#----------------------------------------------------------------------------
class FileMapCache(object):
    """LRU cache."""
    def __init__(self, cap):
        self.cap = cap
        self.circ = CacheItem()
        self.map = {}

    def __getitem__(self, key):
        it = self.map[key]
        it.insertAfter(self.circ)
        return self.map[key].data

    def __setitem__(self, key, data):
        try:
            it = self.map[key]
        except KeyError:
            if len(self.map) < self.cap:
                it = CacheItem(key)
            else:
                it = self.circ.prev
                del self.map[it.key]
                it.key = key
            self.map[key] = it
        it.data = data
        it.insertAfter(self.circ)

    def __delitem__(self, key):
        it = self.map[key]
        it.unlink()
        del self.map[key]
        del it

    def __len__(self):
        return len(self.map)

    def __contains__(self, key):
        return key in self.map


#----------------------------------------------------------------------------
# FileMap
#----------------------------------------------------------------------------
class FileMap(object):
    """Wrap a file object and provide LRU cached memory-mapped access."""
    CHUNK_SIZE = (8 << 10)
    CHUNK_MASK = ~(CHUNK_SIZE - 1)
    DEFAULT_CACHE_CAPACITY = 500

    def __init__(self, f, cache=None):
        if isinstance(f, str):
            self.f = file(f)
        else:
            self.f = f
        if cache is None:
            self.cache = FileMapCache(self.DEFAULT_CACHE_CAPACITY)
        else:
            self.cache = cache
        self._size = None

    def _getsize(self):
        if self._size is None:
            self.f.seek(0,2)
            self._size = self.f.tell()
        return self._size

    size = property(_getsize)

    def close(self):
        self._size = 0
        self.f.close()
        del self.f
        del self.cache

    def __len__(self):
        return self.size

    def __getitem__(self, idx):
        if not isinstance(idx, slice):
            if not 0 <= idx < self.size:
                raise IndexError('Out of bounds: %r'%idx)
            begin = idx
            end = idx + 1
        else:
            assert idx.step is None or idx.step == 1
            begin = idx.start or 0
            end = idx.stop or self.size

        if begin < 0:
            begin = self.size + begin
        if end < 0:
            end = self.size + end

        if begin < 0:
            begin = 0
        if end > self.size:
            end = self.size

        result = ''
        while begin < end:
            base = begin & self.CHUNK_MASK

            key = (self,base)
            if key not in self.cache:
                self.f.seek(base)
                self.cache[key] = self.f.read(self.CHUNK_SIZE)

            result += self.cache[key][begin - base : end - base]
            begin = base + self.CHUNK_SIZE

        return result

def open(fn):
    return FileMap(fn)
