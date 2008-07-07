#!/usr/bin/env python
#
# Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
# Created 2006-03-29
# 
# This file is part of Alien.
# 
# Alien is free software; you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or any later version.
# 
# Alien is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

from alien.prim import MsbInt
from util.xmlfmt import xml

#----------------------------------------------------------------------------
# ArrayBase
#----------------------------------------------------------------------------
class ArrayBase(object):
    pass


#----------------------------------------------------------------------------
# Array
#----------------------------------------------------------------------------
def Array(etype, ever=None, ltype=MsbInt, lver=None):

    class Array(ArrayBase):
        def __init__(self, buf, base=0, ver=None):
            self._buf = buf
            if callable(ltype):
                self._arrlen = ltype(buf, base, lver)
                self._size = ltype._sizeof(lver)
                if callable(self._size):
                    self._size = self._size(self._arrlen)
            else:
                self._arrlen = ltype
                self._size = 0
            self._arrbase = base + self._size
            esize = etype._sizeof(ever)
            if callable(esize):
                off = 0
                offsets = []
                for i in range(self._arrlen):
                    offsets.append(off)
                    off += esize(etype(self._buf, self._arrbase + off, ever))
                self._size += off
                self._offsetof = lambda idx: offsets[idx]
            else:
                self._size = self._size + esize * self._arrlen
                self._offsetof = lambda idx: idx * esize

        def _sizeof(cls, ver=None):
            return lambda obj: obj._size
        _sizeof = classmethod(_sizeof)

        def __getitem__(self, idx):
            if not -self._arrlen <= idx < self._arrlen:
                raise IndexError('index out of bounds: %r' % idx)
            if idx < 0:
                idx += self._arrlen
            off = self._offsetof(idx)
            return etype(self._buf, self._arrbase + off, ever)

        def __len__(self):
            return self._arrlen

        def __iter__(self):
            for i in range(self._arrlen):
                off = self._offsetof(i)
                yield etype(self._buf, self._arrbase + off, ever)

        def __repr__(self):
            return '[%s]' % ', '.join([repr(x) for x in self])

        def __str__(self):
            return '[%s]' % ', '.join([str(x) for x in self])

        def __xml__(self):
            return ''.join(map(xml, self))

    return Array


#----------------------------------------------------------------------------
# FixedArray
#----------------------------------------------------------------------------
def FixedArray(nelem, etype, ever=None):
    return Array(etype, ever, nelem)


#----------------------------------------------------------------------------
# DataArray (deprecated)
#   TODO: merge this into Array()
#----------------------------------------------------------------------------
class DataArray(object):
    def __init__(self, s, dtype, dver=None, base=0, nelem=None):
        self.s = s
        self.base = base
        self.dtype = dtype
        self.dver = dver
        self.dsize = dtype._sizeof(dver)
        self.nelem = nelem
        if callable(self.dsize):
            raise TypeError('Must have fixed size data type')

    def __getitem__(self, idx):
        if idx < 0 or (self.nelem is not None and idx >= self.nelem):
            raise IndexError('DataArray index out of range: %r' % idx)
        return self.dtype(self.s, self.base + idx * self.dsize, self.dver)

    def __len__(self):
        if self.nelem is not None:
            return self.nelem
        else:
            return (len(self.s) - self.base) / self.dsize

    def __iter__(self):
        off = self.base
        for i in range(len(self)):
            yield self.dtype(self.s, off, self.dver)
            off += self.dsize
