#!/usr/bin/env python
#
# Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
# Created 2006-03-29
#
# This file is part of KDI.
# 
# KDI is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free
# Software Foundation; either version 2 of the License, or any later
# version.
# 
# KDI is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
# License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.

import cStringIO, gzip
from alien.prim import MsbInt
from util.xmlfmt import xml

#----------------------------------------------------------------------------
# GZippedData
#----------------------------------------------------------------------------
def GZippedData(ztype, zver=None, ltype=MsbInt, lver=None):
    
    class GZippedData(object):
        def __init__(self, buf, base=0, ver=None):
            self._buf = buf
            if callable(ltype):
                self._gzlen = ltype(buf, base, lver)
                lsize = ltype._sizeof(lver)
                if callable(lsize):
                    self._gzbase = base + lsize(self._gzlen)
                else:
                    self._gzbase = base + lsize
            else:
                self._gzlen = ltype
                self._gzbase = base
            self._size = self._gzbase + self._gzlen - base

        def _sizeof(cls, ver=None):
            if callable(ltype):
                return lambda obj: obj._size
            else:
                return ltype
        _sizeof = classmethod(_sizeof)

        def __get_raw_zipped(self):
            return self._buf[self._gzbase : self._gzbase + self._gzlen]
        rawZipped = property(__get_raw_zipped)

        def __get_raw_unzipped(self):
            return gzip.GzipFile('','r',9,cStringIO.StringIO(self.rawZipped)).read()
        rawUnzipped = property(__get_raw_unzipped)

        def __get_unzipped(self):
            return ztype(self.rawUnzipped, 0, zver)
        unzipped = property(__get_unzipped)

        def __xml__(self):
            return xml(self.unzipped)
        

    return GZippedData

