#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/alien/gzdata.py#1 $
#
# Created 2006/03/29
#
# Copyright 2006 Cosmix Corporation.  All rights reserved.
# Cosmix PROPRIETARY and CONFIDENTIAL.

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

